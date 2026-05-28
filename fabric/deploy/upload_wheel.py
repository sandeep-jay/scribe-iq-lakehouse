"""Upload the core wheel to a Fabric Environment via the Fabric REST API.

Stub. Session 5 fills in the real API calls. The shape below is the contract:
CI builds `core/dist/scribe_iq_lakehouse_core-X.Y.Z-py3-none-any.whl`, this
script authenticates with a Service Principal (env vars set in the
`fabric-prod` GitHub Environment), and PUTs the wheel onto the configured
Fabric Environment ID. Notebooks then `import core.*` from that environment.

See ADR-018 and fabric/docs/DEPLOYMENT.md.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def upload_wheel(wheel_path: Path, environment_id: str) -> None:
    """Upload a wheel to a Fabric Environment.

    Args:
        wheel_path: Local path to the built core wheel.
        environment_id: Fabric Environment ID (`FABRIC_ENVIRONMENT_ID` env var).

    Raises:
        NotImplementedError: Session 5 work — REST API integration pending.
    """
    raise NotImplementedError(
        "Fabric REST upload pending Session 5. Required env vars: "
        "FABRIC_TENANT_ID, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, "
        "FABRIC_WORKSPACE_ID, FABRIC_ENVIRONMENT_ID."
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--wheel", type=Path, required=True, help="Path to built core wheel")
    ap.add_argument(
        "--environment-id",
        default=os.environ.get("FABRIC_ENVIRONMENT_ID"),
        help="Fabric Environment ID (or set FABRIC_ENVIRONMENT_ID)",
    )
    args = ap.parse_args()
    if not args.environment_id:
        ap.error("--environment-id or FABRIC_ENVIRONMENT_ID required")
    upload_wheel(args.wheel, args.environment_id)


if __name__ == "__main__":
    main()
