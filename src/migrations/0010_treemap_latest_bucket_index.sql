create index if not exists market_category_snapshot_1h_coverage_provider_bucket_idx
  on agg.market_category_snapshot_1h (coverage_mode, provider_code, bucket_ts desc);
