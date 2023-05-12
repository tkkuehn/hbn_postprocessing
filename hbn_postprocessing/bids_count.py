"""Figure out how many relevant files there are."""

from __future__ import annotations

import glob
import os
import re
from argparse import ArgumentParser
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


def count_all_files(
    bids_dir: os.PathLike[str] | str,
    out_dir: os.PathLike[str] | str,
) -> None:
    """Write CSVs with relevant image counts."""
    bids_path = Path(bids_dir)
    file_count_df = pd.DataFrame(
        [
            count_files(bids_path, sub_dir.name.split("-")[1])
            for sub_dir in glob_dir(
                bids_path,
                "sub-*",
                filter_=lambda path: path.is_dir(),
            )
        ],
    )
    out_path = Path(out_dir)
    file_count_df.to_csv(out_path / "BIDS-count_all.csv", sep=",", index=False)
    exclude_df = file_count_df.loc[
        lambda df: (df["t1_files"] == 0) | (df["fmap_files"] == 0),
        :,
    ]
    exclude_df.to_csv(out_path / "BIDS-count_exclude.csv", sep=",", index=False)
    file_count_df.loc[lambda df: ~df["id"].isin(exclude_df["id"]), :].to_csv(
        out_path / "BIDS-count_include.csv",
        sep=",",
        index=False,
    )


def check_html(
    fmriprep_out_dir: os.PathLike[str] | str,
    participants_path: os.PathLike[str] | str,
    out_dir: os.PathLike[str] | str,
) -> None:
    """Write a set of summary CSVs checking if participants have an HTML file."""
    html_files = {file_.stem for file_ in glob_dir(fmriprep_out_dir, "*.html*")}
    participants = pd.read_csv(participants_path)
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


def _process_job_file(file_: Path) -> dict[str, str | float]:
    with file_.open() as file_content:
        out_content = file_content.read()
    subj_id = out_content.partition("participant_label ")[2][0:12]
    size_kb = file_.stat().st_size / 1000
    return {"name": file_.name, "p_id": subj_id, "size_kb": size_kb}


STARTED_THRESHOLD_KB = 10
COMPLETED_THRESHOLD_KB = 400


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


CONFOUNDS_PATTERN = re.compile(
    r"sub-(?P<subject>[a-zA-Z\d]+)_task-(?P<task>[a-zA-Z\d]+)"
    r"(?:_run-(?P<run>\d+))?_desc-(?P<description>[a-zA-Z\d]+)"
    r"_(?P<suffix>[a-zA-Z\d]+).tsv",
)


def get_framewise_displacement(
    subj_dir: os.PathLike[str] | str,
) -> dict[str, str | float]:
    """Get the framewise displacement in each task for a subject."""
    subj_path = Path(subj_dir)
    tsvs = glob_dir(subj_path / "func", "*.tsv*")
    sub_dict: dict[str, str | float] = {"id": subj_path.name}
    for tsv in tsvs:
        match = re.match(CONFOUNDS_PATTERN, tsv.name)
        if not match:
            raise ValueError
        task = match.group("task")
        run = f"_run-{match.group('run')}" if match.group("run") else ""
        task_run = f"{task}{run}"
        data = pd.read_csv(tsv, sep="\t", header=0)
        sub_dict[task_run] = data["framewise_displacement"].tail(-1).mean()
    return sub_dict


def exclude_by_motion(
    bids_dir: os.PathLike[str] | str,
    out_dir: os.PathLike[str] | str,
) -> None:
    """Find outliers by framewise displacement per task."""
    subj_dirs = glob_dir(bids_dir, "sub*", filter_=lambda path: path.is_dir())
    displacement_df = pd.DataFrame(
        [get_framewise_displacement(subj_dir) for subj_dir in subj_dirs],
    )
    group_fds = displacement_df.iloc[:, 1:].mean(axis=0)
    group_sds = displacement_df.iloc[:, 1:].mean(axis=0)
    upper_lims = group_fds + (2 * group_sds)
    for task_run in set(displacement_df.columns) - {"id"}:
        displacement_df = displacement_df.assign(
            **{
                f"{task_run}_is_outlier": displacement_df[task_run]
                > upper_lims[task_run],
            },
        )
        displacement_df = displacement_df.drop(task_run, axis=1)
    displacement_df.to_csv(
        Path(out_dir) / "motion-outliers_all.csv",
        sep=",",
        index=False,
    )


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

    count_all_files(args.bids_dir, args.out_dir)
    check_html(
        args.fmriprep_dir, Path(args.bids_dir) / "participants.tsv", args.out_dir,
    )
    check_jobs(args.jobs_dir, args.out_dir)
    exclude_by_motion(args.fmriprep_dir, args.out_dir)


if __name__ == "__main__":
    main()
