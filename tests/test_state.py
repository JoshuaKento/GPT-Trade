import os
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from edgar.state import load_state, save_state


def test_state_roundtrip(tmp_path):
    path = tmp_path / "state.json"
    state = {"0000000000": {"a1"}}
    save_state(state, path)
    loaded = load_state(path)
    assert loaded == state
