CREATE SCHEMA "agg";
--> statement-breakpoint
CREATE TABLE "agg"."market_liquidity_1h" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "agg"."market_liquidity_1h_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"market_id" bigint NOT NULL,
	"instrument_id" bigint NOT NULL,
	"bucket_ts" timestamp with time zone NOT NULL,
	"avg_spread" numeric(9, 6),
	"avg_bid_depth_top5" numeric(18, 6),
	"avg_ask_depth_top5" numeric(18, 6),
	"bbo_presence_rate" numeric(9, 6) NOT NULL,
	"sample_count" integer NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "agg"."market_price_1h" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "agg"."market_price_1h_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"market_id" bigint NOT NULL,
	"instrument_id" bigint NOT NULL,
	"bucket_ts" timestamp with time zone NOT NULL,
	"open" numeric(9, 6) NOT NULL,
	"high" numeric(9, 6) NOT NULL,
	"low" numeric(9, 6) NOT NULL,
	"close" numeric(9, 6) NOT NULL,
	"points" integer NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "agg"."market_liquidity_1h" ADD CONSTRAINT "market_liquidity_1h_market_id_market_id_fk" FOREIGN KEY ("market_id") REFERENCES "core"."market"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agg"."market_liquidity_1h" ADD CONSTRAINT "market_liquidity_1h_instrument_id_instrument_id_fk" FOREIGN KEY ("instrument_id") REFERENCES "core"."instrument"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agg"."market_price_1h" ADD CONSTRAINT "market_price_1h_market_id_market_id_fk" FOREIGN KEY ("market_id") REFERENCES "core"."market"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agg"."market_price_1h" ADD CONSTRAINT "market_price_1h_instrument_id_instrument_id_fk" FOREIGN KEY ("instrument_id") REFERENCES "core"."instrument"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "market_liquidity_1h_instrument_bucket_uq" ON "agg"."market_liquidity_1h" USING btree ("instrument_id","bucket_ts");--> statement-breakpoint
CREATE INDEX "market_liquidity_1h_market_bucket_idx" ON "agg"."market_liquidity_1h" USING btree ("market_id","bucket_ts");--> statement-breakpoint
CREATE INDEX "market_liquidity_1h_bucket_idx" ON "agg"."market_liquidity_1h" USING btree ("bucket_ts");--> statement-breakpoint
CREATE UNIQUE INDEX "market_price_1h_instrument_bucket_uq" ON "agg"."market_price_1h" USING btree ("instrument_id","bucket_ts");--> statement-breakpoint
CREATE INDEX "market_price_1h_market_bucket_idx" ON "agg"."market_price_1h" USING btree ("market_id","bucket_ts");--> statement-breakpoint
CREATE INDEX "market_price_1h_bucket_idx" ON "agg"."market_price_1h" USING btree ("bucket_ts");