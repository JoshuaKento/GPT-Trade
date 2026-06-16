"""Regression tests for documented gpt_trader public imports."""


def test_documented_batch_filing_processor_public_import():
    from gpt_trader import BatchFilingProcessor
    from gpt_trader.filing_processor_db import BatchFilingProcessor as expected

    assert BatchFilingProcessor is expected
