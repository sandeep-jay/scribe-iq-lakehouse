"""One-patient end-to-end medallion walkthrough — for screencast / portfolio demo.

Picks (or accepts) one synthetic patient and renders their journey through every
layer of the lakehouse:

    Bronze   → bundle file inventory + FHIR resource-type counts + sample JSON
    Parse    → ``FHIRBundleParser.parse_bundle`` extracted records (the pure step)
    Silver   → that patient's rows from the Delta tables (typed, deduped, CDC-on)
    Gold     → one ``gold.encounter_summary`` row with the SOAP note rendered

Designed for screen recording: rich panels, deliberate pacing (``--pause``),
syntax-highlighted JSON, and a final summary card. Pure-Python — only needs
``[local,dev]`` (no ``[orchestration]`` / no Dagster). Mirrors what the Dagster
asset metadata renders, but for a terminal audience.

Usage:
    .venv/bin/python -m scripts.demo_walkthrough             # auto-pick a good demo patient
    .venv/bin/python -m scripts.demo_walkthrough --patient-id <uuid>
    .venv/bin/python -m scripts.demo_walkthrough --pause 1.5  # 1.5s between sections
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import pyarrow.compute as pc
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.table import Table

from core.platform.factory import get_platform
from core.preview import (
    bundle_resource_counts,
    gold_encounter_card,
    sample_md,
    schema_md,
)
from core.transforms.fhir_parser import FHIRBundleParser

console = Console()


def _find_anchor_patient(silver_patient, silver_condition, silver_medication_request, silver_soap):
    """Pick a deterministic 'good demo' patient with rich downstream signal.

    Criteria: ≥3 conditions, ≥3 medications, ≥1 SOAP note. Returns the first
    matching ``patient_id`` (sorted) so the choice is reproducible across runs.
    """
    cond_counts = silver_condition.group_by("patient_id").aggregate([("condition_id", "count")])
    med_counts = silver_medication_request.group_by("patient_id").aggregate(
        [("medication_request_id", "count")]
    )
    soap_counts = silver_soap.group_by("patient_id").aggregate([("note_id", "count")])

    cond_ok = pc.filter(
        cond_counts["patient_id"], pc.greater_equal(cond_counts["condition_id_count"], 3)
    )
    med_ok = pc.filter(
        med_counts["patient_id"],
        pc.greater_equal(med_counts["medication_request_id_count"], 3),
    )
    soap_ok = pc.filter(
        soap_counts["patient_id"], pc.greater_equal(soap_counts["note_id_count"], 1)
    )

    ids = set(cond_ok.to_pylist()) & set(med_ok.to_pylist()) & set(soap_ok.to_pylist())
    if not ids:
        # Fallback: any patient that has a SOAP note.
        ids = set(soap_ok.to_pylist())
    if not ids:
        # Last resort: first patient_id we can find.
        return silver_patient["patient_id"][0].as_py()
    return sorted(ids)[0]


def _find_bundle_for_patient(bronze_root: Path, patient_id: str) -> Path | None:
    """Find the Bronze FHIR bundle JSON for ``patient_id``.

    Coherent encodes the patient UUID as the trailing component of each bundle
    filename (e.g. ``Abe604_Frami345_<uuid>.json``), so we can match by suffix
    without parsing every bundle. Falls back to ``None`` if no match.
    """
    for path in (bronze_root / "fhir").glob("cohort=*/*.json"):
        if path.stem.endswith(patient_id):
            return path
    # Brute fallback: parse Patient.id from each bundle (slow; bounded).
    for path in (bronze_root / "fhir").glob("cohort=*/*.json"):
        try:
            for entry in json.loads(path.read_text()).get("entry", []):
                res = entry.get("resource") or {}
                if res.get("resourceType") == "Patient" and res.get("id") == patient_id:
                    return path
        except Exception:  # noqa: BLE001 — best-effort scan
            continue
    return None


def _pause(seconds: float) -> None:
    """Sleep ``seconds`` (skip if 0) — pacing for screen recording."""
    if seconds > 0:
        time.sleep(seconds)


def _patient_rows(table, patient_id: str, limit: int | None = None):
    """Filter a Silver Arrow table to one patient's rows, with optional row cap."""
    mask = pc.equal(table["patient_id"], patient_id)
    filtered = table.filter(mask)
    return filtered.slice(0, limit) if limit else filtered


def main() -> int:
    """Render the medallion walkthrough for one patient."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--patient-id", help="Specific Synthea patient_id; auto-picks if omitted.")
    parser.add_argument(
        "--pause", type=float, default=0.0, help="Seconds between sections (screencast pacing)."
    )
    args = parser.parse_args()

    platform = get_platform()
    bronze_root = platform.root / "bronze"

    # -------------------------------------------------- silver — read once, reuse
    console.print(Rule("[bold cyan]scribe-iq-lakehouse — one patient through the medallion"))
    console.print(
        f"[dim]Platform: {platform.name}  ·  Storage: {platform.root}[/dim]\n",
    )

    with console.status("[cyan]Reading Silver tables…[/cyan]"):
        silver_patient = platform.read_silver("patient")
        silver_encounter = platform.read_silver("encounter")
        silver_observation = platform.read_silver("observation")
        silver_condition = platform.read_silver("condition")
        silver_medication = platform.read_silver("medication_request")
        silver_soap = platform.read_silver("soap_note")

    # -------------------------------------------------- pick anchor patient
    patient_id = args.patient_id or _find_anchor_patient(
        silver_patient, silver_condition, silver_medication, silver_soap
    )
    matching = _patient_rows(silver_patient, patient_id)
    if matching.num_rows == 0:
        console.print(f"[red]Patient {patient_id!r} not found in silver.patient.[/red]")
        return 1
    patient_row = matching.slice(0, 1).to_pylist()[0]
    console.print(
        Panel.fit(
            f"Anchor patient: [bold]{patient_id}[/bold]\n"
            f"Gender: {patient_row.get('gender')}  ·  Birth: {patient_row.get('birth_date')}",
            title="Demo subject",
            border_style="cyan",
        )
    )
    _pause(args.pause)

    # -------------------------------------------------- 1. BRONZE
    console.print(Rule("[bold yellow]1. Bronze — raw FHIR JSON, append-only"))
    bundle_path = _find_bundle_for_patient(bronze_root, patient_id)
    if not bundle_path:
        console.print(f"[red]No Bronze bundle found for {patient_id}.[/red]")
        return 1
    bundle = json.loads(bundle_path.read_text())
    counts = bundle_resource_counts(bundle)
    table = Table(title=None, show_header=True, header_style="dim", box=None)
    table.add_column("Resource type", style="bold")
    table.add_column("Count", justify="right")
    for rtype, count in counts.items():
        table.add_row(rtype, f"{count}")
    bundle_title = (
        f"[yellow]{bundle_path.name}[/yellow]  ·  "
        f"{bundle_path.stat().st_size:,} bytes  ·  "
        f"{sum(counts.values())} FHIR resources"
    )
    console.print(Panel(table, title=bundle_title, border_style="yellow"))

    # Show one Patient resource as syntax-highlighted JSON.
    patient_resource = next(
        (
            e["resource"]
            for e in bundle.get("entry", [])
            if (e.get("resource") or {}).get("resourceType") == "Patient"
        ),
        None,
    )
    if patient_resource:
        snippet = json.dumps(patient_resource, indent=2)
        if len(snippet) > 1200:
            snippet = snippet[:1200] + "\n  ...(truncated)"
        console.print(
            Panel(
                Syntax(snippet, "json", theme="monokai", line_numbers=False),
                title="[yellow]Sample FHIR Patient resource[/yellow]",
                border_style="dim",
            )
        )
    _pause(args.pause)

    # -------------------------------------------------- 2. PARSE (pure transform)
    console.print(Rule("[bold magenta]2. Parse — FHIRBundleParser (pure, no I/O, returns dicts)"))
    parsed = FHIRBundleParser().parse_bundle(bundle)
    parse_table = Table(show_header=True, header_style="dim", box=None)
    parse_table.add_column("Silver table", style="bold")
    parse_table.add_column("Records parsed", justify="right")
    for name in sorted(parsed):
        parse_table.add_row(name, f"{len(parsed[name])}")
    console.print(
        Panel(parse_table, title="Parsed records by Silver table", border_style="magenta")
    )
    _pause(args.pause)

    # -------------------------------------------------- 3. SILVER
    console.print(Rule("[bold green]3. Silver — typed Delta tables (CDC enabled)"))
    for label, t in [
        ("silver.patient", _patient_rows(silver_patient, patient_id)),
        ("silver.encounter (first 5)", _patient_rows(silver_encounter, patient_id, 5)),
        ("silver.observation (first 5)", _patient_rows(silver_observation, patient_id, 5)),
        ("silver.condition (first 5)", _patient_rows(silver_condition, patient_id, 5)),
        ("silver.medication_request (first 5)", _patient_rows(silver_medication, patient_id, 5)),
    ]:
        console.print(
            Panel(
                Markdown(sample_md(t)),
                title=f"[green]{label}[/green]  ·  {t.num_rows} row(s) shown",
                border_style="green",
            )
        )
    # Schema reference for one Silver table — proves the typed shape.
    console.print(
        Panel(
            Markdown(schema_md(silver_encounter)),
            title="[green]silver.encounter schema[/green]",
            border_style="dim",
        )
    )
    _pause(args.pause)

    # -------------------------------------------------- 4. GOLD
    console.print(Rule("[bold blue]4. Gold — gold.encounter_summary (1 row per encounter)"))
    gold = platform.read_gold("encounter_summary")
    gold_rows = _patient_rows(gold, patient_id)
    if gold_rows.num_rows == 0:
        console.print(f"[red]No Gold rows for {patient_id} (unexpected).[/red]")
        return 1
    # Prefer a row with a SOAP note for the rendered card.
    soap_col = gold_rows.column("soap_note_text")
    idx = next((i for i in range(gold_rows.num_rows) if soap_col[i].as_py()), 0)
    sample = gold_rows.slice(idx, 1).to_pylist()[0]
    gold_title = (
        f"[blue]gold.encounter_summary[/blue]  ·  "
        f"{gold_rows.num_rows} encounters for this patient"
    )
    console.print(
        Panel(Markdown(gold_encounter_card(sample)), title=gold_title, border_style="blue")
    )
    _pause(args.pause)

    # -------------------------------------------------- summary
    console.print(Rule("[bold]Summary"))
    summary = Table(show_header=True, header_style="dim", box=None)
    summary.add_column("Layer")
    summary.add_column("This patient")
    summary.add_column("Whole corpus", justify="right")
    summary.add_row(
        "Bronze bundle",
        f"{bundle_path.name}",
        f"{sum(1 for _ in (bronze_root / 'fhir').glob('cohort=*/*.json'))} files",
    )
    summary.add_row(
        "silver.encounter",
        str(_patient_rows(silver_encounter, patient_id).num_rows),
        f"{silver_encounter.num_rows:,}",
    )
    summary.add_row(
        "silver.observation",
        str(_patient_rows(silver_observation, patient_id).num_rows),
        f"{silver_observation.num_rows:,}",
    )
    summary.add_row(
        "silver.condition",
        str(_patient_rows(silver_condition, patient_id).num_rows),
        f"{silver_condition.num_rows:,}",
    )
    summary.add_row(
        "silver.medication_request",
        str(_patient_rows(silver_medication, patient_id).num_rows),
        f"{silver_medication.num_rows:,}",
    )
    summary.add_row("gold.encounter_summary", str(gold_rows.num_rows), f"{gold.num_rows:,}")
    console.print(summary)
    console.print(
        f"\n[dim]Anchor: --patient-id {patient_id}  ·  reproduce: "
        f"python -m scripts.demo_walkthrough --patient-id {patient_id}[/dim]"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
