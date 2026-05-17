import argparse
from .config import load_config
from .logging_ import setup_logging

from .jobs.ods_to_stage import run_ods_to_stage
from .jobs.stage_clean import run_stage_clean
from .jobs.stage_split import run_stage_split
from .jobs.finalize_instock import run_finalize_instock
from .jobs.finalize_unqualified import run_finalize_unqualified

STEPS = {
    "ods2stage": run_ods_to_stage,
    "stage_clean": run_stage_clean,
    "split": run_stage_split,
    "final_instock": run_finalize_instock,
    "final_unqualified": run_finalize_unqualified,
}

def main():
    parser = argparse.ArgumentParser(prog="pat-etl")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run pipeline steps")
    p_run.add_argument("--config", required=True, help="Path to YAML config")
    p_run.add_argument(
        "--steps",
        default="ods2stage,stage_clean,split",
        help="Comma-separated steps: " + ",".join(STEPS.keys()),
    )

    args = parser.parse_args()
    cfg = load_config(args.config)
    setup_logging()

    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    for s in steps:
        if s not in STEPS:
            raise SystemExit(f"Unknown step: {s}. Allowed: {list(STEPS)}")
        print(f"\n=== STEP: {s} ===")
        STEPS[s](cfg)

if __name__ == "__main__":
    main()
