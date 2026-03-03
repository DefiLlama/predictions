create index if not exists trade_event_ts_idx on raw.trade_event (ts);
create index if not exists market_close_time_idx on core.market (close_time);
