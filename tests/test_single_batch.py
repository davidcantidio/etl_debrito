import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from treat.treat_pipeline import execute_pipeline, TreatPipeline
from load import dest_writer
import treat.treat_pipeline as pipeline
from googleapiclient import discovery

class DummyFetcher:
    def __init__(self):
        self.calls = 0
        self.cache = {}
    def get(self, names, as_frame=True):
        self.calls += 1
        result = {}
        for n in names:
            df = self.cache.get(n)
            if df is None:
                df = pd.DataFrame({"ID": [1]})
                self.cache[n] = df
            result[n] = df
        return result
    def get_cached(self, names, as_frame=True):
        return {n: self.cache[n] for n in names}
    def open_worksheet(self, name):
        return MagicMock(title=name)


@pytest.mark.unit
def test_single_batch_get_and_update(monkeypatch):
    service = MagicMock()
    batch_call = service.spreadsheets.return_value.values.return_value.batchUpdate
    batch_call.return_value.execute.return_value = {}
    monkeypatch.setattr(discovery, "build", lambda *a, **k: service)
    monkeypatch.setattr(dest_writer, "_build_service", lambda c: service)

    monkeypatch.setattr(TreatPipeline, "run", lambda self, df: df)
    monkeypatch.setattr(pipeline, "prefetch_meta", lambda f, s: dest_writer._HEADERS.update({dest_writer.DESTINATION_SHEETS["geral"]: ["ID"]}))

    fetcher = DummyFetcher()
    execute_pipeline(["metaGeral"], "creds.json", "xyz", write_back_origin=False, fetcher=fetcher)

    assert fetcher.calls == 1
    assert batch_call.call_count == 1

