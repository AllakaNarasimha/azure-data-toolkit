"""Minimal replacements inside existing market-data managers.

Keep response flattening and partition path generation in the consuming app.
"""
from azure_data_toolkit import DataLake

lake = DataLake.from_env()

# QuoteCacheManager.save(): after creating a one-row `df` and `blob_name`:
# lake.upsert(blob_name, df, keys=["symbol", "source", "fetched_at"])

# OptionChainCacheManager.save_fyers_responses_batch(): after building `df`:
# lake.upsert(blob_name, df, keys=["symbol", "source", "expiry_timestamp", "fetched_at"])

# QuoteCacheManager.load() or OptionChainCacheManager.load():
# return lake.read(blob_name, missing_ok=True)

# Expiry side table: intentionally replace the metadata file, not append.
# lake.write(expiry_blob_name, expiry_df)

# Historical candle manager, same existing Hive path convention:
# lake.upsert(blob_name, candles_df, keys=["timestamp"], sort_by="timestamp")
# history = lake.read(blob_name, missing_ok=True, timestamp_col="timestamp", start="2026-10-01", end="2026-10-08")

# Delta table: table path (not an individual .parquet file):
# lake.upsert("tables/ohlcv", candles_df, keys=["symbol", "interval", "timestamp"], format="delta", partition_by=["symbol", "interval", "year", "month"])
# lake.read("tables/ohlcv", format="delta", filters={"symbol": "NIFTY50"})
