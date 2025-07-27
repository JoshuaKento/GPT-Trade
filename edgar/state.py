"""Local state file helpers with enhanced error handling and validation."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Set


def load_state(path: str) -> Dict[str, Set[str]]:
    """Load processing state from JSON file.

    Args:
        path: Path to state file

    Returns:
        Dictionary mapping CIK to set of processed accession numbers

    Raises:
        ValueError: If state file contains invalid data
        RuntimeError: If file cannot be read
    """
    logger = logging.getLogger(__name__)

    # Convert path to string if it's a Path object
    path_str = (
        str(path) if hasattr(path, "__fspath__") or hasattr(path, "__str__") else path
    )

    if not path_str or not path_str.strip():
        raise ValueError("State file path cannot be empty")

    file_path = Path(path)

    if not file_path.exists():
        logger.info(f"State file {path} does not exist, starting with empty state")
        return {}

    try:
        with open(file_path, "r", encoding="utf-8") as fp:
            data = json.load(fp)

        if not isinstance(data, dict):
            raise ValueError("State file must contain a JSON object")

        # Validate and convert data
        state = {}
        for cik, accessions in data.items():
            if not isinstance(cik, str):
                raise ValueError(f"CIK must be string, got {type(cik)}")

            if not isinstance(accessions, list):
                raise ValueError(
                    f"Accessions for CIK {cik} must be list, got {type(accessions)}"
                )

            # Convert to set and validate accession format
            accession_set = set()
            for acc in accessions:
                if not isinstance(acc, str):
                    logger.warning(f"Invalid accession type for CIK {cik}: {type(acc)}")
                    continue
                accession_set.add(acc)

            state[cik] = accession_set

        logger.info(f"Loaded state for {len(state)} CIKs from {path}")
        return state

    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in state file {path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load state file {path}: {e}")


def save_state(state: Dict[str, Set[str]], path: str) -> None:
    """Save processing state to JSON file.

    Args:
        state: Dictionary mapping CIK to set of processed accession numbers
        path: Path to save state file

    Raises:
        ValueError: If state data is invalid
        RuntimeError: If file cannot be written
    """
    logger = logging.getLogger(__name__)

    # Convert path to string if it's a Path object
    path_str = (
        str(path) if hasattr(path, "__fspath__") or hasattr(path, "__str__") else path
    )

    if not path_str or not path_str.strip():
        raise ValueError("State file path cannot be empty")

    if not isinstance(state, dict):
        raise ValueError("State must be a dictionary")

    try:
        # Create directory if it doesn't exist
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert sets to sorted lists for JSON serialization
        data = {}
        total_accessions = 0

        for cik, accessions in state.items():
            if not isinstance(cik, str):
                raise ValueError(f"CIK must be string, got {type(cik)}")

            if not isinstance(accessions, set):
                raise ValueError(
                    f"Accessions for CIK {cik} must be set, got {type(accessions)}"
                )

            # Sort for consistent output
            data[cik] = sorted(list(accessions))
            total_accessions += len(accessions)

        # Write to temporary file first, then rename (atomic operation)
        temp_path = file_path.with_suffix(file_path.suffix + ".tmp")

        with open(temp_path, "w", encoding="utf-8") as fp:
            json.dump(data, fp, indent=2, sort_keys=True)

        # Atomic rename
        temp_path.replace(file_path)

        logger.info(
            f"Saved state for {len(data)} CIKs ({total_accessions} accessions) to {path}"
        )

    except Exception as e:
        # Clean up temp file if it exists
        temp_path = Path(path).with_suffix(Path(path).suffix + ".tmp")
        if temp_path.exists():
            try:
                temp_path.unlink()
            except:
                pass

        raise RuntimeError(f"Failed to save state file {path}: {e}")


def validate_state(state: Dict[str, Set[str]]) -> None:
    """Validate state data structure.

    Args:
        state: State dictionary to validate

    Raises:
        ValueError: If state structure is invalid
    """
    if not isinstance(state, dict):
        raise ValueError("State must be a dictionary")

    for cik, accessions in state.items():
        if not isinstance(cik, str):
            raise ValueError(f"CIK must be string, got {type(cik)}")

        if not isinstance(accessions, set):
            raise ValueError(
                f"Accessions for CIK {cik} must be set, got {type(accessions)}"
            )

        # Basic CIK validation
        if not cik.strip():
            raise ValueError("CIK cannot be empty")

        # Basic accession validation
        for acc in accessions:
            if not isinstance(acc, str):
                raise ValueError(f"Accession must be string, got {type(acc)}")
            if not acc.strip():
                raise ValueError("Accession cannot be empty")
