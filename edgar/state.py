"""Local state file helpers."""
from typing import Dict, Set
import os
import json


def load_state(path: str) -> Dict[str, Set[str]]:
    if not os.path.exists(path):
        return {}
    with open(path, "r") as fp:
        data = json.load(fp)
    return {cik: set(vals) for cik, vals in data.items()}


def save_state(state: Dict[str, Set[str]], path: str) -> None:
    data = {cik: sorted(list(vals)) for cik, vals in state.items()}
    with open(path, "w") as fp:
        json.dump(data, fp, indent=2)
