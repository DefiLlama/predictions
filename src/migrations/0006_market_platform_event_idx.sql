CREATE INDEX IF NOT EXISTS "market_platform_event_idx" ON "core"."market" USING btree ("platform_id", "event_id");
