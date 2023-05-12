"""Utilities for handling HBN data."""

from __future__ import annotations

import glob
import os
from collections.abc import Callable
from itertools import chain
from pathlib import Path


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
