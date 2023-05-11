"""Figure out how many relevant files there are."""

from __future__ import annotations

import glob
import os
from collections.abc import Callable
from dataclasses import dataclass
from itertools import chain
from pathlib import Path

import pandas as pd


def glob_dir(
    path_spec: os.PathLike[str] | str,
    *keywords: str,
    filter_: Callable[[Path], bool] | None = None,
) -> list[Path]:
    """List all the files matching the keyword glob.

    Parameters
    ----------
    path_spec
        Directory to glob
    keywords
        The type of file (e.g. "*T1w.nii.gz")
    filter_
        Function that decides if a path should be accepted
    """
    path = Path(path_spec)
    return [
        Path(dir_)
        for dir_ in chain.from_iterable(
            [glob.glob(str(path / keyword)) for keyword in keywords],
        )
        if (filter_(Path(dir_)) if filter_ else True)
    ]


@dataclass
class SearchSpec:
    """A spec to find a kind of file in a directory."""

    datatype: str
    img_type: str
    glob: str

    def count_files(self, datatype_dir: os.PathLike[str] | str) -> dict[str, int | str]:
        """Find all the files in the directory that match the glob."""
        files = glob_dir(datatype_dir, self.glob)
        if files:
            return {self.img_type: "yes", f"{self.img_type}_files": len(files)}
        return {self.img_type: "no", f"{self.img_type}_files": 0}


DATATYPE_SPECS = {
    spec.datatype: spec
    for spec in [
        SearchSpec(datatype="anat", img_type="t1", glob="*T1w.nii.gz*"),
        SearchSpec(datatype="func", img_type="func", glob="*bold.nii.gz*"),
        SearchSpec(datatype="fmap", img_type="fmap", glob="*fMRI_epi.nii.gz*"),
    ]
}


def count_files(bids_dir: os.PathLike[str] | str, subj_id: str) -> dict[str, int | str]:
    """Count the T1w, bold, and fMRI_epi files for the subject."""
    subj_dir = Path(bids_dir) / f"sub-{subj_id}"
    content = glob_dir(subj_dir, "*", filter_=lambda path: path.is_dir())
    sub_dict: dict[str, int | str] = {"id": subj_id}

    for datatype_dir in content:
        datatype = datatype_dir.name
        if datatype not in DATATYPE_SPECS:
            continue
        sub_dict.update(DATATYPE_SPECS[datatype].count_files(datatype_dir))

    return sub_dict


def check_html(
    fmriprep_out_dir: os.PathLike[str] | str,
    participants: pd.DataFrame,
    out_dir: os.PathLike[str] | str,
) -> None:
    """Write a set of summary CSVs checking if participants have an HTML file."""
    html_files = {file_.stem for file_ in glob_dir(fmriprep_out_dir, "*.html*")}
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


def check_jobs(jobs_dir: os.PathLike[str] | str) -> None:
    out_files = glob_dir(jobs_dir, "*.out*")
    for file_ in out_files:
        with file_.open() as file_content:
            out_content = file_content.read()
        subj_id = out_content.partition("participant_label ")[2][0:12]
        size_kb = file_.stat().st_size / 1000
