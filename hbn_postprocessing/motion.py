"""Tools for dealing with head motion data."""

from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd

from hbn_postprocessing.utils import glob_dir

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
