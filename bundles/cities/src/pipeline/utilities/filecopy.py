"""Utilities to copy workspace files to a Databricks volume path."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

VOLUME_ROOT = os.environ.get("DATABRICKS_VOLUME_ROOT", "/Volumes")


def copy_workspace_to_volume(
    root_path: str | os.PathLike[str],
    catalog_name: str,
    schema_name: str,
    volume_name: str,
) -> Path:
    """Copy a file or directory from the workspace to a volume location.

    Args:
        root_path: Path to the workspace file or directory to copy.
        catalog_name: Unity Catalog name (e.g. "uc_meta_prod").
        schema_name: Schema name inside the catalog (e.g. "config").
        volume_name: Volume name inside the schema (e.g. "configfiles").

    Returns:
        The destination path where content was copied.

    Raises:
        FileNotFoundError: If the source path does not exist.
        ValueError: If any of the catalog, schema, or volume names are empty.
    """

    if not all([catalog_name, schema_name, volume_name]):
        raise ValueError("catalog_name, schema_name, and volume_name are required.")

    source = Path(root_path).expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(f"Source path does not exist: {source}")

    destination_root = Path(VOLUME_ROOT) / catalog_name / schema_name / volume_name
    destination_root.mkdir(parents=True, exist_ok=True)

    if source.is_dir():
        shutil.copytree(source, destination_root, dirs_exist_ok=True)
        return destination_root

    destination_file = destination_root / source.name
    shutil.copy2(source, destination_file)
    return destination_file