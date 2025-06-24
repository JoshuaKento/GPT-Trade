"""tqdm progress bar with fallback when not installed."""
try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency
    def tqdm(*args, **kwargs):  # type: ignore
        class _Dummy:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                pass
            def update(self, n=1):
                pass
            def set_postfix(self, *_, **__):
                pass
        return _Dummy()
    def _tqdm_write(msg: str) -> None:
        print(msg)
    tqdm.write = _tqdm_write  # type: ignore
__all__ = ["tqdm"]
