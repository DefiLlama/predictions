CREATE SCHEMA "core";
--> statement-breakpoint
CREATE SCHEMA "ops";
--> statement-breakpoint
CREATE SCHEMA "raw";
--> statement-breakpoint
CREATE TABLE "core"."event" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "core"."event_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"platform_id" bigint NOT NULL,
	"event_ref" varchar(256) NOT NULL,
	"title" text,
	"category" varchar(128),
	"start_time" timestamp with time zone,
	"end_time" timestamp with time zone,
	"status" varchar(64),
	"raw_json" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "ops"."ingest_checkpoint" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "ops"."ingest_checkpoint_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"provider_code" varchar(64) NOT NULL,
	"job_name" varchar(128) NOT NULL,
	"cursor_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "core"."instrument" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "core"."instrument_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"market_id" bigint NOT NULL,
	"platform_id" bigint NOT NULL,
	"instrument_ref" varchar(256) NOT NULL,
	"outcome_label" varchar(128),
	"outcome_index" integer,
	"is_primary" boolean DEFAULT true NOT NULL,
	"raw_json" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "ops"."job_run_log" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "ops"."job_run_log_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"request_id" varchar(128),
	"provider_code" varchar(64) NOT NULL,
	"job_name" varchar(128) NOT NULL,
	"started_at" timestamp with time zone DEFAULT now() NOT NULL,
	"finished_at" timestamp with time zone,
	"status" varchar(32) NOT NULL,
	"rows_upserted" integer DEFAULT 0 NOT NULL,
	"rows_skipped" integer DEFAULT 0 NOT NULL,
	"error_text" text
);
--> statement-breakpoint
CREATE TABLE "core"."market" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "core"."market_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"platform_id" bigint NOT NULL,
	"event_id" bigint,
	"market_ref" varchar(256) NOT NULL,
	"market_uid" varchar(320) NOT NULL,
	"title" text,
	"status" varchar(64) NOT NULL,
	"close_time" timestamp with time zone,
	"volume_24h" numeric(24, 6),
	"liquidity" numeric(24, 6),
	"raw_json" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "core"."market_scope" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "core"."market_scope_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"platform_id" bigint NOT NULL,
	"market_id" bigint NOT NULL,
	"rank" integer NOT NULL,
	"reason" varchar(128) NOT NULL,
	"computed_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "raw"."orderbook_top" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "raw"."orderbook_top_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"instrument_id" bigint NOT NULL,
	"ts" timestamp with time zone NOT NULL,
	"best_bid" numeric(9, 6),
	"best_ask" numeric(9, 6),
	"spread" numeric(9, 6),
	"bid_depth_top5" numeric(18, 6),
	"ask_depth_top5" numeric(18, 6),
	"raw_json" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "core"."platform" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "core"."platform_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"code" varchar(64) NOT NULL,
	"name" varchar(128) NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "raw"."price_point_5m" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "raw"."price_point_5m_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"instrument_id" bigint NOT NULL,
	"ts" timestamp with time zone NOT NULL,
	"price" numeric(9, 6) NOT NULL,
	"source" varchar(128) NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "core"."event" ADD CONSTRAINT "event_platform_id_platform_id_fk" FOREIGN KEY ("platform_id") REFERENCES "core"."platform"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."instrument" ADD CONSTRAINT "instrument_market_id_market_id_fk" FOREIGN KEY ("market_id") REFERENCES "core"."market"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."instrument" ADD CONSTRAINT "instrument_platform_id_platform_id_fk" FOREIGN KEY ("platform_id") REFERENCES "core"."platform"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."market" ADD CONSTRAINT "market_platform_id_platform_id_fk" FOREIGN KEY ("platform_id") REFERENCES "core"."platform"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."market" ADD CONSTRAINT "market_event_id_event_id_fk" FOREIGN KEY ("event_id") REFERENCES "core"."event"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."market_scope" ADD CONSTRAINT "market_scope_platform_id_platform_id_fk" FOREIGN KEY ("platform_id") REFERENCES "core"."platform"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."market_scope" ADD CONSTRAINT "market_scope_market_id_market_id_fk" FOREIGN KEY ("market_id") REFERENCES "core"."market"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "raw"."orderbook_top" ADD CONSTRAINT "orderbook_top_instrument_id_instrument_id_fk" FOREIGN KEY ("instrument_id") REFERENCES "core"."instrument"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "raw"."price_point_5m" ADD CONSTRAINT "price_point_5m_instrument_id_instrument_id_fk" FOREIGN KEY ("instrument_id") REFERENCES "core"."instrument"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "event_platform_ref_uq" ON "core"."event" USING btree ("platform_id","event_ref");--> statement-breakpoint
CREATE INDEX "event_platform_idx" ON "core"."event" USING btree ("platform_id");--> statement-breakpoint
CREATE UNIQUE INDEX "ingest_checkpoint_provider_job_uq" ON "ops"."ingest_checkpoint" USING btree ("provider_code","job_name");--> statement-breakpoint
CREATE UNIQUE INDEX "instrument_market_ref_uq" ON "core"."instrument" USING btree ("market_id","instrument_ref");--> statement-breakpoint
CREATE INDEX "instrument_market_idx" ON "core"."instrument" USING btree ("market_id");--> statement-breakpoint
CREATE INDEX "instrument_platform_idx" ON "core"."instrument" USING btree ("platform_id");--> statement-breakpoint
CREATE INDEX "job_run_log_provider_job_started_idx" ON "ops"."job_run_log" USING btree ("provider_code","job_name","started_at");--> statement-breakpoint
CREATE INDEX "job_run_log_request_idx" ON "ops"."job_run_log" USING btree ("request_id");--> statement-breakpoint
CREATE UNIQUE INDEX "market_platform_ref_uq" ON "core"."market" USING btree ("platform_id","market_ref");--> statement-breakpoint
CREATE UNIQUE INDEX "market_uid_uq" ON "core"."market" USING btree ("market_uid");--> statement-breakpoint
CREATE INDEX "market_platform_idx" ON "core"."market" USING btree ("platform_id");--> statement-breakpoint
CREATE INDEX "market_status_idx" ON "core"."market" USING btree ("status");--> statement-breakpoint
CREATE UNIQUE INDEX "market_scope_platform_market_uq" ON "core"."market_scope" USING btree ("platform_id","market_id");--> statement-breakpoint
CREATE INDEX "market_scope_platform_rank_idx" ON "core"."market_scope" USING btree ("platform_id","rank");--> statement-breakpoint
CREATE UNIQUE INDEX "orderbook_top_instrument_ts_uq" ON "raw"."orderbook_top" USING btree ("instrument_id","ts");--> statement-breakpoint
CREATE INDEX "orderbook_top_ts_idx" ON "raw"."orderbook_top" USING btree ("ts");--> statement-breakpoint
CREATE UNIQUE INDEX "platform_code_uq" ON "core"."platform" USING btree ("code");--> statement-breakpoint
CREATE UNIQUE INDEX "price_point_5m_instrument_ts_uq" ON "raw"."price_point_5m" USING btree ("instrument_id","ts");--> statement-breakpoint
CREATE INDEX "price_point_5m_ts_idx" ON "raw"."price_point_5m" USING btree ("ts");