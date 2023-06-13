"""Tools to process fmriprep stdout/stderr."""

import os
from pathlib import Path

import pandas as pd

from hbn_postprocessing.utils import glob_dir

STARTED_THRESHOLD_KB = 10
COMPLETED_THRESHOLD_KB = 4000


def _process_job_file(file_: Path) -> dict[str, str | float]:
    with file_.open() as file_content:
        out_content = file_content.read()
    subj_id = out_content.partition("participant_label ")[2][0:12]
    size_kb = file_.stat().st_size / 1000
    return {"name": file_.name, "p_id": subj_id, "size_kb": size_kb}


def check_jobs(
    jobs_dir: os.PathLike[str] | str,
    out_dir: os.PathLike[str] | str,
) -> None:
    """Parse a dir of ".out" files to check for incomplete jobs."""
    out_files = glob_dir(jobs_dir, "*.out*")
    file_info = [_process_job_file(file_) for file_ in out_files]
    size_df = pd.DataFrame(
        {
            "file_name": [file_["name"] for file_ in file_info],
            "p_id": [file_["p_id"] for file_ in file_info],
            "size_kb": [file_["size_kb"] for file_ in file_info],
        },
    ).loc[lambda df: df["p_id"].str.startswith("NDA"), :]
    max_size = (
        size_df.groupby("p_id")
        .max()
        .reset_index()
        .assign(
            status=lambda df: df.size_kb.map(
                lambda size_kb: "not started"
                if size_kb < STARTED_THRESHOLD_KB
                else (
                    "partial/error"
                    if size_kb < COMPLETED_THRESHOLD_KB
                    else "likely complete"
                ),
            ),
        )
    )

    out_path = Path(out_dir)
    max_size.to_csv(out_path / "out-size_all.csv")
    max_size.loc[max_size.status != "likely_complete", :].to_csv(
        out_path / "out-size_incomp.csv",
        sep=",",
        index=False,
    )
    max_size.loc[max_size.status == "likely complete", :].to_csv(
        out_path / "out-size_comp.csv",
        sep=",",
        index=False,
    )
