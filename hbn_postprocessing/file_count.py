"""Tools to count relevant files in the source directory."""

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from hbn_postprocessing.utils import glob_dir


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
    sub_dict: dict[str, int | str] = {"participant_id": f"sub-{subj_id}"}

    for datatype_dir in content:
        datatype = datatype_dir.name
        if datatype not in DATATYPE_SPECS:
            continue
        sub_dict.update(DATATYPE_SPECS[datatype].count_files(datatype_dir))

    return sub_dict


def count_all_files(
    bids_dir: os.PathLike[str] | str,
    out_dir: os.PathLike[str] | str,
) -> pd.DataFrame:
    """Write CSVs with relevant image counts."""
    bids_path = Path(bids_dir)
    file_count_df = (
        pd.DataFrame(
            [
                count_files(bids_path, sub_dir.name.split("-")[1])
                for sub_dir in glob_dir(
                    bids_path,
                    "sub-*",
                    filter_=lambda path: path.is_dir(),
                )
            ],
        )
        .astype({"participant_id": pd.StringDtype()})
        .set_index("participant_id")
    )
    out_path = Path(out_dir)
    file_count_df.to_csv(out_path / "BIDS-count_all.csv", sep=",")
    exclude_df = file_count_df.loc[
        lambda df: (df["t1_files"] == 0) | (df["fmap_files"] == 0),
        :,
    ]
    exclude_df.to_csv(out_path / "BIDS-count_exclude.csv", sep=",")
    file_count_df.loc[
        lambda df: ~df.index.isin(exclude_df.index),
        :,
    ].to_csv(
        out_path / "BIDS-count_include.csv",
        sep=",",
    )
    return file_count_df
