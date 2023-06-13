"""Figure out how many relevant files there are."""

from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path

from hbn_postprocessing.file_count import count_all_files
from hbn_postprocessing.html import check_html
from hbn_postprocessing.jobs import check_jobs
from hbn_postprocessing.motion import exclude_by_motion


def gen_parser() -> ArgumentParser:
    """Generate the parser to use for this app."""
    parser = ArgumentParser()
    parser.add_argument("bids_dir", help="Raw BIDS directory")
    parser.add_argument("fmriprep_dir", help="fmriprep output dir")
    parser.add_argument("jobs_dir", help="Directory with .out files")
    parser.add_argument("out_dir", help="Output directory")
    return parser


def main() -> None:
    """Run all relevant tasks."""
    args = gen_parser().parse_args()

    file_count_df = count_all_files(args.bids_dir, args.out_dir)
    html_check_df = check_html(
        args.fmriprep_dir,
        Path(args.bids_dir) / "participants.tsv",
        args.out_dir,
    )
    out_size_df = check_jobs(args.jobs_dir, args.out_dir)
    motion_outliers_df = exclude_by_motion(args.fmriprep_dir, args.out_dir)
    file_count_df.join(
        [html_check_df, out_size_df, motion_outliers_df], how="outer",
    ).to_csv(
        Path(args.out_dir) / "overall_summary.csv",
        sep=",",
    )


if __name__ == "__main__":
    main()
