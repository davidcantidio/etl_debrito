import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from transform.load.dest_writer import (_EXISTING_IDS, _HEADERS, DESTINATION_SHEETS,
                              write_back_batch)


@pytest.mark.unit
def test_batch_update_single_call_multiple_tabs():
    service = MagicMock()
    batch_call = service.spreadsheets.return_value.values.return_value.batchUpdate
    batch_call.return_value.execute.return_value = {}

    _HEADERS.clear()
    _EXISTING_IDS.clear()
    for sheet in (
        DESTINATION_SHEETS["idade"],
        DESTINATION_SHEETS["genero"],
        DESTINATION_SHEETS["regiao"],
    ):
        _HEADERS[sheet] = ["ID", "valor"]
        _EXISTING_IDS[sheet] = set()

    df = pd.DataFrame({"ID": [1], "valor": [42]})
    frames = {"idade": df, "genero": df, "regiao": df}

    write_back_batch(
        frames,
        creds_path="creds.json",
        spreadsheet_id="xyz",
        service=service,
    )

    assert batch_call.call_count == 1
    body = batch_call.call_args.kwargs["body"]
    assert "data" in body
    assert len(body["data"]) >= 3
