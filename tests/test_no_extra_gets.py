import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from googleapiclient import discovery

import transform.transform.transform_pipeline as pipeline
from transform.load import dest_writer, origin_writer
from transform.transform.transform_pipeline import TreatPipeline, execute_pipeline
from transform.transform.utils.preprocess_utils import preprocess_origin


class DummyFetcher:
    def __init__(self):
        self.calls = 0
        self.cache = {}
        self.header_cache = {}

    def get(self, names, as_frame=True):
        self.calls += 1
        result = {}
        for n in names:
            df = self.cache.get(n)
            if df is None:
                df = pd.DataFrame({"ID": [1]})
                self.cache[n] = df
            result[n] = df
            self.header_cache[n] = df.columns.tolist()
        return result

    def get_cached(self, names, as_frame=True):
        return {n: self.cache[n] for n in names}

    def open_worksheet(self, name):
        return ws


@pytest.mark.unit
def test_no_extra_header_get(monkeypatch):
    service = MagicMock()
    monkeypatch.setattr(discovery, "build", lambda *a, **k: service)
    monkeypatch.setattr(dest_writer, "_build_service", lambda c: service)

    # worksheet mock to capture row_values calls
    global ws
    ws = MagicMock()
    ws.row_values = MagicMock(return_value=["ID"])
    ws.batch_update = MagicMock()
    ws._properties = {"gridProperties": {"frozenRowCount": 0}}
    ws.title = "metaGeral"
    monkeypatch.setattr(pipeline, "get_worksheet", lambda *a, **k: ws)
    monkeypatch.setattr(origin_writer, "write_back_df", lambda **k: None)

    # minimal run just preprocess + write_back_origin
    def simple_run(self, df_raw):
        df_prep = preprocess_origin(
            df_raw,
            worksheet=self.worksheet_origem,
            write_back=self.write_back,
        )
        origin_writer.write_back_origin(
            df_raw=df_raw,
            df_ok=df_prep,
            creds_path=self.creds_path,
            spreadsheet_id=self.spreadsheet_id,
            worksheet=self.worksheet_origem,
            write_back=self.write_back,
            skip_if_written=True,
            header=self.fetcher.header_cache[self.sheet_name],
            dry_run=False,
        )
        return df_prep

    monkeypatch.setattr(TreatPipeline, "run", simple_run)
    monkeypatch.setattr(
        pipeline,
        "prefetch_meta",
        lambda f, s: dest_writer._HEADERS.update(
            {dest_writer.DESTINATION_SHEETS["geral"]: ["ID"]}
        ),
    )

    fetcher = DummyFetcher()
    execute_pipeline(["metaGeral"], "creds.json", "xyz", fetcher=fetcher)

    assert fetcher.calls == 1
    assert ws.row_values.call_count == 0
    import builtins

    assert getattr(builtins, "_wb_origin_done") == {"metaGeral"}
