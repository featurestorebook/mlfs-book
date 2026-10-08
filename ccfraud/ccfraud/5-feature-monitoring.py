#!/usr/bin/env python
"""
Feature monitoring: drift of the transaction amount, checked every hour with PSI.

Creates (once; re-runs are no-ops) a feature monitoring configuration on the
`credit_card_transactions` feature group that Hopsworks runs on a schedule as a
monitoring job. Each run computes the distribution of `amount` over the detection
window (the last day of transactions, by event time `ts`) and over the reference
window (the week before that), and compares them with the Population Stability
Index (PSI). A PSI above the threshold (0.2, the usual "significant shift" cut-off)
marks the run as a detected shift, visible on the feature group's monitoring tab
and usable by an alert on `feature_monitor_shift_detected`.

    python ccfraud/5-feature-monitoring.py            # create the hourly config
    python ccfraud/5-feature-monitoring.py --run-now  # ... and run the job once right away
    python ccfraud/5-feature-monitoring.py --replace  # recreate it with new parameters

Requires the feature monitoring service to be enabled on the Hopsworks cluster. PSI
monitoring also requires kll=True in the feature group's statistics configuration, which
the script sets (statistics stay disabled, so inserts launch no statistics job).
"""

import argparse
import sys
from pathlib import Path

import hopsworks

current_file = Path(__file__).absolute()
ccfraud_project_dir = current_file.parent.parent  # ccfraud/
root_dir = ccfraud_project_dir.parent  # mlfs-book/

FG_NAME = "credit_card_transactions"
FG_VERSION = 1
CONFIG_NAME = "amount_psi_hourly"
HOURLY = "0 0 * ? * * *"  # Quartz: at minute 0 of every hour


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Hourly PSI drift monitoring of the transaction amount")
    parser.add_argument("--feature", default="amount", help="Feature to monitor (default: amount)")
    parser.add_argument("--threshold", type=float, default=0.2,
                        help="PSI above which a shift is detected (default: 0.2)")
    parser.add_argument("--cron", default=HOURLY, help=f"Quartz cron of the monitoring job (default: '{HOURLY}')")
    parser.add_argument("--detection-window", default="1d",
                        help="Detection window: the transactions of the last ... (default: 1d)")
    parser.add_argument("--reference-offset", default="1w1d",
                        help="Reference window starts this long ago (default: 1w1d)")
    parser.add_argument("--reference-length", default="1w",
                        help="Length of the reference window (default: 1w, i.e. the week before the detection window)")
    parser.add_argument("--run-now", action="store_true", help="Run the monitoring job once immediately")
    parser.add_argument("--replace", action="store_true",
                        help="Delete an existing configuration of the same name and create it again")
    parser.add_argument("--env-file", default=None,
                        help="Path to .env file, when running outside Hopsworks (default: <root>/.env)")
    return parser.parse_args(argv)


def ensure_kll_flag(fg) -> None:
    """Set the `kll` flag of the feature group's statistics configuration.

    Hopsworks refuses a distribution (PSI) monitoring configuration unless the feature group's
    statistics configuration has kll=True (the KLL sketch the distribution is estimated with).
    Statistics stay disabled (enabled=False), so an insert still launches no statistics job:
    the monitoring job profiles the detection and reference windows itself when it runs.
    """
    from hsfs.statistics_config import StatisticsConfig

    sc = fg.statistics_config
    if getattr(sc, "kll", False):
        return
    fg.statistics_config = StatisticsConfig(enabled=sc.enabled, correlations=sc.correlations,
                                            histograms=sc.histograms, exact_uniqueness=sc.exact_uniqueness,
                                            columns=sc.columns, kll=True)
    fg.update_statistics_config()
    print(f"Set kll=True in the statistics configuration of {fg.name} v{fg.version}")


def main(argv=None):
    args = parse_args(argv)
    env_file = Path(args.env_file) if args.env_file else root_dir / ".env"
    if env_file.exists():
        sys.path.insert(0, str(root_dir))
        from mlfs import config
        config.HopsworksSettings(_env_file=str(env_file))

    project = hopsworks.login()
    fs = project.get_feature_store()
    fg = fs.get_feature_group(FG_NAME, version=FG_VERSION)
    if fg is None:
        sys.exit(f"{FG_NAME} v{FG_VERSION} not found: run `inv datamart` first")

    ensure_kll_flag(fg)
    existing = [c for c in fg.get_feature_monitoring_configs() if c.name == CONFIG_NAME]
    if existing and args.replace:
        for c in existing:
            c.delete()
            print(f"Deleted monitoring configuration {CONFIG_NAME}")
        existing = []

    if existing:
        config = existing[0]
        print(f"Monitoring configuration {CONFIG_NAME} already exists on {FG_NAME} v{FG_VERSION} "
              f"(use --replace to recreate it)")
    else:
        config = (
            fg.create_feature_monitoring(
                name=CONFIG_NAME,
                description=(f"PSI of `{args.feature}` over the last {args.detection_window} vs the "
                             f"{args.reference_length} before; shift when PSI > {args.threshold}"),
                cron_expression=args.cron,
            )
            .with_detection_window(time_offset=args.detection_window)
            .with_reference_window(time_offset=args.reference_offset, window_length=args.reference_length)
            .compare_on_distribution(feature_name=args.feature, metric="PSI", threshold=args.threshold)
            .save()
        )
        print(f"Created monitoring configuration {CONFIG_NAME} on {FG_NAME} v{FG_VERSION}")

    print(f"  feature:    {args.feature}")
    print(f"  metric:     PSI, shift when > {args.threshold}")
    print(f"  schedule:   {args.cron}")
    print(f"  detection:  last {args.detection_window} of transactions (by event time ts)")
    print(f"  reference:  {args.reference_length} ending {args.detection_window} ago")
    print(f"  job:        {config.job_name}")

    if args.run_now:
        print("Running the monitoring job once...")
        config.run_once()
        print(f"Monitoring job {config.job_name} started: check its execution in the Jobs UI")


if __name__ == "__main__":
    main()
