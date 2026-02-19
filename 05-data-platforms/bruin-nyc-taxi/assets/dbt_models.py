"""@bruin
name: dbt_models
type: python
@bruin"""

import subprocess
from pathlib import Path

def main():
    # Always resolve relative to THIS file location
    pipeline_root = Path(__file__).resolve().parents[1]   # .../bruin-nyc-taxi
    dbt_path = pipeline_root / "assets" / "ny_taxi_dbt_core"

    if not dbt_path.exists():
        raise FileNotFoundError(f"dbt project path not found: {dbt_path}")

    subprocess.run(["dbt", "deps"], cwd=str(dbt_path), check=True)
    subprocess.run(["dbt", "build"], cwd=str(dbt_path), check=True)

if __name__ == "__main__":
    main()
