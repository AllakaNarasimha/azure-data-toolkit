import pandas as pd
import pytest
from azure_data_toolkit.frames import merge_frames, filter_frame, resample_ohlcv


def test_merge_upsert_overrides_existing_and_is_idempotent():
    original = pd.DataFrame([{'timestamp': 1, 'close': 10}, {'timestamp': 2, 'close': 20}])
    updates = pd.DataFrame([{'timestamp': 2, 'close': 22}, {'timestamp': 3, 'close': 30}])
    once = merge_frames(original, updates, ['timestamp'], 'timestamp')
    twice = merge_frames(once, updates, ['timestamp'], 'timestamp')
    assert once.to_dict('records') == twice.to_dict('records')
    assert once['close'].tolist() == [10, 22, 30]


def test_merge_rejects_null_keys():
    with pytest.raises(ValueError):
        merge_frames(pd.DataFrame(), pd.DataFrame([{'id': None}]), ['id'])


def test_merge_rejects_missing_keys():
    with pytest.raises(KeyError):
        merge_frames(pd.DataFrame(), pd.DataFrame([{'id': 1}]), ['other'])


def test_filter_inclusive_dates_and_membership():
    frame = pd.DataFrame({'symbol': ['A', 'B', 'A'], 'timestamp': ['2026-10-01T09:15:00Z', '2026-10-02T09:15:00Z', '2026-10-03T09:15:00Z']})
    result = filter_frame(frame, {'symbol': ['A']}, timestamp_col='timestamp', start='2026-10-02', end='2026-10-03T09:15:00Z')
    assert result['timestamp'].tolist() == ['2026-10-03T09:15:00Z']


def test_resample_ohlcv_five_minutes():
    ts = pd.date_range('2026-10-08 09:15:00', periods=5, freq='min')
    frame = pd.DataFrame({'symbol': ['NIFTY50'] * 5, 'timestamp': ts, 'open': [1, 2, 3, 4, 5], 'high': [2, 3, 4, 5, 6], 'low': [0, 1, 2, 3, 4], 'close': [2, 3, 4, 5, 6], 'volume': [10] * 5})
    result = resample_ohlcv(frame)
    assert len(result) == 1
    assert result.iloc[0][['open', 'high', 'low', 'close', 'volume']].tolist() == [1, 6, 0, 6, 50]


def test_resample_rejects_missing_columns():
    with pytest.raises(ValueError):
        resample_ohlcv(pd.DataFrame([{'timestamp': '2026-10-08'}]))
