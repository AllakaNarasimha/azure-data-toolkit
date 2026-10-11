"""Pure DataFrame operations: no storage or broker dependencies."""
from __future__ import annotations
from collections.abc import Sequence
import pandas as pd


def merge_frames(existing: pd.DataFrame, incoming: pd.DataFrame, keys: Sequence[str], sort_by: str | None = None, revision_col: str | None = None) -> pd.DataFrame:
    if not keys:
        raise ValueError("upsert requires at least one business key")
    if incoming is None or incoming.empty:
        return existing.copy().reset_index(drop=True)
    missing_incoming = set(keys) - set(incoming.columns)
    if missing_incoming:
        raise KeyError(f"missing key columns: {sorted(missing_incoming)}")
    if revision_col:
        for frame in (existing, incoming):
            if not frame.empty and (revision_col not in frame or frame[revision_col].isna().any()):
                raise ValueError("revision column must exist and contain no nulls")
    if incoming[list(keys)].isna().any().any():
        raise ValueError("incoming business keys cannot be null")
    frames = [f for f in (existing, incoming) if not f.empty]
    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    missing = set(keys) - set(result.columns)
    if missing:
        raise KeyError(f"missing key columns: {sorted(missing)}")
    if revision_col:
        # Stable ordering gives the highest revision precedence per business key.
        # Equal revisions retain the historical last-writer-wins behavior.
        result = result.sort_values(revision_col, kind="stable")
    result = result.drop_duplicates(subset=list(keys), keep="last")
    if sort_by:
        if sort_by not in result:
            raise ValueError(f"missing sort column: {sort_by}")
        result = result.sort_values(sort_by, kind="stable")
    return result.reset_index(drop=True)


def filter_frame(df: pd.DataFrame, filters: dict | None = None, *, timestamp_col: str | None = None, start=None, end=None, timestamp_unit: str | None = None) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    mask = pd.Series(True, index=df.index)
    for col, val in (filters or {}).items():
        if col not in df:
            raise KeyError(col)
        mask &= df[col].isin(val) if isinstance(val, (tuple, list, set)) else df[col].eq(val)
    if start is not None or end is not None:
        if not timestamp_col:
            raise ValueError("timestamp_col required for start/end")
        if pd.api.types.is_numeric_dtype(df[timestamp_col]) and timestamp_unit is None:
            raise ValueError("numeric timestamps require timestamp_unit (s, ms, us, or ns)")
        times = pd.to_datetime(df[timestamp_col], utc=True, unit=timestamp_unit)
        if start is not None:
            mask &= times >= pd.to_datetime(start, utc=True)
        if end is not None:
            mask &= times <= pd.to_datetime(end, utc=True)
    return df.loc[mask].reset_index(drop=True)


def resample_ohlcv(df: pd.DataFrame, *, interval: str = "5min", timestamp_col: str = "timestamp", group_by: tuple[str, ...] = ("symbol",), origin: str = "start_day", offset: str = "15min") -> pd.DataFrame:
    """Generic OHLCV resampling; caller is responsible for session/calendar filtering.

    Defaults anchor Indian intraday 5m bars at :15; input timestamps must be bar start.
    Partial bars are retained; callers must decide whether to exclude them.
    """
    if df.empty:
        return df.copy()
    required = {timestamp_col, "open", "high", "low", "close", "volume"}
    if required - set(df.columns):
        raise ValueError(f"missing OHLCV columns: {sorted(required - set(df.columns))}")
    keys = [c for c in group_by if c in df.columns]
    work = df.copy()
    work[timestamp_col] = pd.to_datetime(work[timestamp_col])
    work = work.sort_values(timestamp_col)
    output = []
    groups = work.groupby(keys, dropna=False, sort=False) if keys else [((), work)]
    for group_key, part in groups:
        bars = (part.set_index(timestamp_col).resample(interval, label="left", closed="left", origin=origin, offset=offset)
                .agg({"open":"first", "high":"max", "low":"min", "close":"last", "volume":"sum"})
                .dropna(subset=["open", "close"]).reset_index())
        if keys:
            vals = group_key if isinstance(group_key, tuple) else (group_key,)
            for key, val in zip(keys, vals):
                bars[key] = val
        output.append(bars)
    return pd.concat(output, ignore_index=True) if output else pd.DataFrame()

