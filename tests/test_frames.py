import pandas as pd
import pytest
from azure_data_toolkit import merge_frames, filter_frame, resample_ohlcv

def test_upsert_last_write_wins():
    old = pd.DataFrame([{"id":1,"price":10},{"id":2,"price":20}])
    new = pd.DataFrame([{"id":1,"price":12},{"id":3,"price":30}])
    result = merge_frames(old, new, ["id"], "id")
    assert result.to_dict("records") == [{"id":1,"price":12},{"id":2,"price":20},{"id":3,"price":30}]

def test_filters():
    df = pd.DataFrame({"symbol":["A","B"],"timestamp":["2026-10-01","2026-10-02"]})
    assert len(filter_frame(df, {"symbol":"B"}, timestamp_col="timestamp", start="2026-10-02")) == 1

def test_ohlcv():
    df = pd.DataFrame({"timestamp":["2026-10-08 09:15", "2026-10-08 09:16"], "symbol":["A","A"], "open":[10,11],"high":[12,13],"low":[9,10],"close":[11,12],"volume":[5,7]})
    out = resample_ohlcv(df)
    assert len(out) == 1
    assert out.iloc[0]["volume"] == 12
    assert out.iloc[0]["high"] == 13

def test_reject_null_key():
    with pytest.raises(ValueError):
        merge_frames(pd.DataFrame(), pd.DataFrame({"id":[None]}), ["id"])
