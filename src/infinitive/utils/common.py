from __future__ import annotations


def normalize_name(value: str) -> str:
	"""Normalize text for consistent identifiers."""

	return "_".join(value.strip().lower().split())