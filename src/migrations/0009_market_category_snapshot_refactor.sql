CREATE TABLE "agg"."market_category_snapshot_1h" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "agg"."market_category_snapshot_1h_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"provider_code" varchar(64) NOT NULL,
	"coverage_mode" varchar(16) DEFAULT 'all' NOT NULL,
	"bucket_ts" timestamp with time zone NOT NULL,
	"market_id" bigint NOT NULL,
	"category_code" varchar(128) NOT NULL,
	"category_label" varchar(128) NOT NULL,
	"volume24h" numeric(24, 6) NOT NULL,
	"liquidity" numeric(24, 6) NOT NULL,
	"status" varchar(64) NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "agg"."market_category_snapshot_1h" ADD CONSTRAINT "market_category_snapshot_1h_market_id_market_id_fk" FOREIGN KEY ("market_id") REFERENCES "core"."market"("id") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
CREATE UNIQUE INDEX "market_category_snapshot_1h_provider_coverage_bucket_market_uq" ON "agg"."market_category_snapshot_1h" USING btree ("provider_code","coverage_mode","bucket_ts","market_id");
--> statement-breakpoint
CREATE INDEX "market_category_snapshot_1h_provider_coverage_bucket_idx" ON "agg"."market_category_snapshot_1h" USING btree ("provider_code","coverage_mode","bucket_ts");
--> statement-breakpoint
CREATE INDEX "market_category_snapshot_1h_bucket_idx" ON "agg"."market_category_snapshot_1h" USING btree ("bucket_ts");
--> statement-breakpoint
CREATE INDEX "market_category_snapshot_1h_provider_category_bucket_idx" ON "agg"."market_category_snapshot_1h" USING btree ("provider_code","category_code","bucket_ts");
--> statement-breakpoint
DROP TABLE "agg"."provider_category_1h";
