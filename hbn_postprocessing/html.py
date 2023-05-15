"""Tools to handle HTML files from fmriprep."""

import os
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from hbn_postprocessing.utils import glob_dir


def check_html(
    fmriprep_out_dir: os.PathLike[str] | str,
    participants_path: os.PathLike[str] | str,
    out_dir: os.PathLike[str] | str,
    subjects: Iterable[str] | None = None,
) -> None:
    """Write a set of summary CSVs checking if participants have an HTML file."""
    html_files = {file_.stem for file_ in glob_dir(fmriprep_out_dir, "*.html*")}
    participants = pd.read_csv(participants_path)
    if subjects:
        missing_subjects = set(subjects)
    matches = participants.assign(
        html=participants["participant_id"]
        .isin(html_files)
        .replace([True, False], ["yes", "no"]),
    )
    out_path = Path(out_dir)
    matches.to_csv(out_path / "html-check_all.csv", sep=",", index=False)
    matches[matches["html"] == "no"].to_csv(
        out_path / "html-check_no.csv",
        sep=",",
        index=False,
    )
    matches[matches["html"] == "yes"].to_csv(
        out_path / "html-check_yes.csv",
        sep=",",
        index=False,
    )
