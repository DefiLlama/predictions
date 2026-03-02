CREATE TABLE "raw"."oi_point_5m" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "raw"."oi_point_5m_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"provider_code" varchar(64) NOT NULL,
	"market_id" bigint NOT NULL,
	"ts" timestamp with time zone NOT NULL,
	"value" numeric(24, 6) NOT NULL,
	"unit" varchar(32) NOT NULL,
	"source" varchar(128) NOT NULL,
	"raw_json" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "raw"."trade_event" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "raw"."trade_event_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"provider_code" varchar(64) NOT NULL,
	"trade_ref" varchar(256) NOT NULL,
	"market_id" bigint NOT NULL,
	"instrument_id" bigint,
	"ts" timestamp with time zone NOT NULL,
	"side" varchar(16),
	"price" numeric(9, 6),
	"qty" numeric(24, 6),
	"notional_usd" numeric(24, 6),
	"trader_ref" varchar(256),
	"source" varchar(128) NOT NULL,
	"raw_json" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "raw"."oi_point_5m" ADD CONSTRAINT "oi_point_5m_market_id_market_id_fk" FOREIGN KEY ("market_id") REFERENCES "core"."market"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "raw"."trade_event" ADD CONSTRAINT "trade_event_market_id_market_id_fk" FOREIGN KEY ("market_id") REFERENCES "core"."market"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "raw"."trade_event" ADD CONSTRAINT "trade_event_instrument_id_instrument_id_fk" FOREIGN KEY ("instrument_id") REFERENCES "core"."instrument"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "oi_point_5m_provider_market_ts_uq" ON "raw"."oi_point_5m" USING btree ("provider_code","market_id","ts");--> statement-breakpoint
CREATE INDEX "oi_point_5m_provider_ts_idx" ON "raw"."oi_point_5m" USING btree ("provider_code","ts");--> statement-breakpoint
CREATE INDEX "oi_point_5m_market_ts_idx" ON "raw"."oi_point_5m" USING btree ("market_id","ts");--> statement-breakpoint
CREATE UNIQUE INDEX "trade_event_provider_trade_ref_uq" ON "raw"."trade_event" USING btree ("provider_code","trade_ref");--> statement-breakpoint
CREATE INDEX "trade_event_provider_ts_idx" ON "raw"."trade_event" USING btree ("provider_code","ts");--> statement-breakpoint
CREATE INDEX "trade_event_market_ts_idx" ON "raw"."trade_event" USING btree ("market_id","ts");--> statement-breakpoint
CREATE INDEX "trade_event_instrument_ts_idx" ON "raw"."trade_event" USING btree ("instrument_id","ts");