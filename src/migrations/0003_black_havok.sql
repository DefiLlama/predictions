CREATE TABLE "core"."category_dim" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "core"."category_dim_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"code" varchar(128) NOT NULL,
	"label" varchar(128) NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "core"."market_category_assignment" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "core"."market_category_assignment_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"market_id" bigint NOT NULL,
	"platform_id" bigint NOT NULL,
	"category_id" bigint NOT NULL,
	"source" varchar(32) NOT NULL,
	"assigned_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "agg"."provider_category_1h" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "agg"."provider_category_1h_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"provider_code" varchar(64) NOT NULL,
	"category_code" varchar(128) NOT NULL,
	"category_label" varchar(128) NOT NULL,
	"bucket_ts" timestamp with time zone NOT NULL,
	"volume24h_total" numeric(24, 6) NOT NULL,
	"volume24h_active" numeric(24, 6) NOT NULL,
	"oi_total" numeric(24, 6) NOT NULL,
	"oi_active" numeric(24, 6) NOT NULL,
	"market_count" integer NOT NULL,
	"active_market_count" integer NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "core"."market_category_assignment" ADD CONSTRAINT "market_category_assignment_market_id_market_id_fk" FOREIGN KEY ("market_id") REFERENCES "core"."market"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."market_category_assignment" ADD CONSTRAINT "market_category_assignment_platform_id_platform_id_fk" FOREIGN KEY ("platform_id") REFERENCES "core"."platform"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."market_category_assignment" ADD CONSTRAINT "market_category_assignment_category_id_category_dim_id_fk" FOREIGN KEY ("category_id") REFERENCES "core"."category_dim"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "category_dim_code_uq" ON "core"."category_dim" USING btree ("code");--> statement-breakpoint
CREATE UNIQUE INDEX "market_category_assignment_market_uq" ON "core"."market_category_assignment" USING btree ("market_id");--> statement-breakpoint
CREATE INDEX "market_category_assignment_platform_category_idx" ON "core"."market_category_assignment" USING btree ("platform_id","category_id");--> statement-breakpoint
CREATE INDEX "market_category_assignment_category_idx" ON "core"."market_category_assignment" USING btree ("category_id");--> statement-breakpoint
CREATE UNIQUE INDEX "provider_category_1h_provider_category_bucket_uq" ON "agg"."provider_category_1h" USING btree ("provider_code","category_code","bucket_ts");--> statement-breakpoint
CREATE INDEX "provider_category_1h_bucket_idx" ON "agg"."provider_category_1h" USING btree ("bucket_ts");--> statement-breakpoint
CREATE INDEX "provider_category_1h_provider_bucket_idx" ON "agg"."provider_category_1h" USING btree ("provider_code","bucket_ts");