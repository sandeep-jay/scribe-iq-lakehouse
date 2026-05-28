"""Upload the core wheel to a Fabric Environment via the Fabric REST API.

CI builds ``dist/scribe_iq_lakehouse_core-X.Y.Z-py3-none-any.whl``, this script
authenticates with a Service Principal (env vars supplied by the
``fabric-prod`` GitHub Environment), uploads the wheel as a staged custom
library, and publishes the Environment so the new wheel takes effect on the
next notebook run.

REST contract (Fabric API ``v1``):
    PUT  /workspaces/{ws}/environments/{env}/staging/libraries
    POST /workspaces/{ws}/environments/{env}/staging/publish

Auth: MSAL ``ConfidentialClientApplication`` against the Fabric resource
``https://api.fabric.microsoft.com``.

See ADR-018, [fabric/docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

_FABRIC_AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
_FABRIC_API = "https://api.fabric.microsoft.com/v1"

_REQUIRED_ENV = (
    "FABRIC_TENANT_ID",
    "FABRIC_CLIENT_ID",
    "FABRIC_CLIENT_SECRET",
    "FABRIC_WORKSPACE_ID",
    "FABRIC_ENVIRONMENT_ID",
)


def _get_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Acquire a Fabric access token via Service Principal (client credentials)."""
    import msal  # lazy — only needed when actually uploading

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=_FABRIC_AUTHORITY.format(tenant_id=tenant_id),
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(scopes=[_FABRIC_SCOPE])
    if "access_token" not in result:
        raise RuntimeError(
            f"Fabric token acquisition failed: {result.get('error_description', result)}"
        )
    return result["access_token"]


def _upload_library(token: str, workspace_id: str, environment_id: str, wheel_path: Path) -> None:
    """PUT the wheel to the Environment's staging libraries area."""
    import requests

    url = (
        f"{_FABRIC_API}/workspaces/{workspace_id}/environments/{environment_id}"
        "/staging/libraries"
    )
    with wheel_path.open("rb") as fh:
        resp = requests.put(
            url,
            headers={"Authorization": f"Bearer {token}"},
            files={"file": (wheel_path.name, fh, "application/octet-stream")},
            timeout=300,
        )
    if resp.status_code not in (200, 202):
        raise RuntimeError(f"Library upload failed [{resp.status_code}]: {resp.text}")


def _publish_environment(token: str, workspace_id: str, environment_id: str) -> dict[str, Any]:
    """POST to publish the staged libraries; returns the publish operation status."""
    import requests

    url = (
        f"{_FABRIC_API}/workspaces/{workspace_id}/environments/{environment_id}" "/staging/publish"
    )
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    if resp.status_code not in (200, 202):
        raise RuntimeError(f"Publish request failed [{resp.status_code}]: {resp.text}")
    return resp.json() if resp.content else {}


def _poll_publish(
    token: str, workspace_id: str, environment_id: str, *, timeout_s: int = 600
) -> None:
    """Poll the environment until its publish state leaves ``Running``."""
    import requests

    url = f"{_FABRIC_API}/workspaces/{workspace_id}/environments/{environment_id}"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        resp.raise_for_status()
        state = (resp.json().get("properties", {}) or {}).get("publishDetails", {}).get("state")
        if state in (None, "Success"):
            return
        if state == "Failed":
            raise RuntimeError(f"Environment publish failed: {resp.json()}")
        time.sleep(10)
    raise TimeoutError(f"Environment publish did not complete within {timeout_s}s")


def upload_wheel(
    wheel_path: Path,
    environment_id: str,
    *,
    tenant_id: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    workspace_id: str | None = None,
    wait: bool = True,
) -> None:
    """Upload a wheel to a Fabric Environment and publish it.

    Args:
        wheel_path: Local path to the built core wheel.
        environment_id: Fabric Environment ID (``FABRIC_ENVIRONMENT_ID``).
        tenant_id: Override for ``FABRIC_TENANT_ID``.
        client_id: Override for ``FABRIC_CLIENT_ID``.
        client_secret: Override for ``FABRIC_CLIENT_SECRET``.
        workspace_id: Override for ``FABRIC_WORKSPACE_ID``.
        wait: Poll until publish completes (set ``False`` for fire-and-forget).
    """
    tenant_id = tenant_id or os.environ["FABRIC_TENANT_ID"]
    client_id = client_id or os.environ["FABRIC_CLIENT_ID"]
    client_secret = client_secret or os.environ["FABRIC_CLIENT_SECRET"]
    workspace_id = workspace_id or os.environ["FABRIC_WORKSPACE_ID"]

    if not wheel_path.exists():
        raise FileNotFoundError(wheel_path)

    print(f"[upload_wheel] acquiring Fabric token (tenant={tenant_id[:8]}…)")
    token = _get_token(tenant_id, client_id, client_secret)

    print(f"[upload_wheel] uploading {wheel_path.name} → env {environment_id}")
    _upload_library(token, workspace_id, environment_id, wheel_path)

    print("[upload_wheel] publishing environment")
    _publish_environment(token, workspace_id, environment_id)

    if wait:
        print("[upload_wheel] waiting for publish to complete")
        _poll_publish(token, workspace_id, environment_id)
        print("[upload_wheel] publish succeeded")


def main() -> None:
    """CLI entry point — parse args, validate env, run :func:`upload_wheel`."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--wheel", type=Path, required=True, help="Path to built core wheel")
    ap.add_argument(
        "--environment-id",
        default=os.environ.get("FABRIC_ENVIRONMENT_ID"),
        help="Fabric Environment ID (or set FABRIC_ENVIRONMENT_ID)",
    )
    ap.add_argument(
        "--no-wait",
        action="store_true",
        help="Skip the publish poll (fire-and-forget)",
    )
    args = ap.parse_args()

    if not args.environment_id:
        ap.error("--environment-id or FABRIC_ENVIRONMENT_ID required")

    missing = [v for v in _REQUIRED_ENV if v != "FABRIC_ENVIRONMENT_ID" and not os.environ.get(v)]
    if missing:
        ap.error(f"missing required env vars: {', '.join(missing)}")

    upload_wheel(args.wheel, args.environment_id, wait=not args.no_wait)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, FileNotFoundError, TimeoutError) as err:
        print(f"[upload_wheel] {err}", file=sys.stderr)
        sys.exit(1)
