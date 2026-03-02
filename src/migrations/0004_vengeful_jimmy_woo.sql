CREATE TABLE "core"."provider_category_dim" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "core"."provider_category_dim_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"platform_id" bigint NOT NULL,
	"source_kind" varchar(64) NOT NULL,
	"code" varchar(128) NOT NULL,
	"label" varchar(128) NOT NULL,
	"is_noise" boolean DEFAULT false NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "core"."provider_category_map" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "core"."provider_category_map_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"platform_id" bigint NOT NULL,
	"source_kind" varchar(64) NOT NULL,
	"source_code" varchar(128) NOT NULL,
	"source_label" varchar(128) NOT NULL,
	"canonical_category_id" bigint NOT NULL,
	"priority" integer DEFAULT 100 NOT NULL,
	"is_active" boolean DEFAULT true NOT NULL,
	"notes" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
DROP INDEX "agg"."provider_category_1h_provider_category_bucket_uq";--> statement-breakpoint
ALTER TABLE "core"."market_category_assignment" ADD COLUMN "canonical_category_id" bigint;--> statement-breakpoint
ALTER TABLE "core"."market_category_assignment" ADD COLUMN "provider_category_id" bigint;--> statement-breakpoint
ALTER TABLE "core"."market_category_assignment" ADD COLUMN "confidence" varchar(16) DEFAULT 'low' NOT NULL;--> statement-breakpoint
ALTER TABLE "agg"."provider_category_1h" ADD COLUMN "group_by" varchar(32) DEFAULT 'sector' NOT NULL;--> statement-breakpoint
ALTER TABLE "agg"."provider_category_1h" ADD COLUMN "source_kind" varchar(64);--> statement-breakpoint
ALTER TABLE "core"."provider_category_dim" ADD CONSTRAINT "provider_category_dim_platform_id_platform_id_fk" FOREIGN KEY ("platform_id") REFERENCES "core"."platform"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."provider_category_map" ADD CONSTRAINT "provider_category_map_platform_id_platform_id_fk" FOREIGN KEY ("platform_id") REFERENCES "core"."platform"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."provider_category_map" ADD CONSTRAINT "provider_category_map_canonical_category_id_category_dim_id_fk" FOREIGN KEY ("canonical_category_id") REFERENCES "core"."category_dim"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "provider_category_dim_platform_source_code_uq" ON "core"."provider_category_dim" USING btree ("platform_id","source_kind","code");--> statement-breakpoint
CREATE INDEX "provider_category_dim_platform_idx" ON "core"."provider_category_dim" USING btree ("platform_id");--> statement-breakpoint
CREATE INDEX "provider_category_dim_source_kind_idx" ON "core"."provider_category_dim" USING btree ("source_kind");--> statement-breakpoint
CREATE UNIQUE INDEX "provider_category_map_platform_source_code_uq" ON "core"."provider_category_map" USING btree ("platform_id","source_kind","source_code");--> statement-breakpoint
CREATE INDEX "provider_category_map_platform_idx" ON "core"."provider_category_map" USING btree ("platform_id");--> statement-breakpoint
CREATE INDEX "provider_category_map_canonical_idx" ON "core"."provider_category_map" USING btree ("canonical_category_id");--> statement-breakpoint
ALTER TABLE "core"."market_category_assignment" ADD CONSTRAINT "market_category_assignment_canonical_category_id_category_dim_id_fk" FOREIGN KEY ("canonical_category_id") REFERENCES "core"."category_dim"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "core"."market_category_assignment" ADD CONSTRAINT "market_category_assignment_provider_category_id_provider_category_dim_id_fk" FOREIGN KEY ("provider_category_id") REFERENCES "core"."provider_category_dim"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
UPDATE "core"."market_category_assignment"
SET "canonical_category_id" = "category_id"
WHERE "canonical_category_id" IS NULL;--> statement-breakpoint
ALTER TABLE "core"."market_category_assignment" ALTER COLUMN "canonical_category_id" SET NOT NULL;--> statement-breakpoint
CREATE INDEX "market_category_assignment_canonical_category_idx" ON "core"."market_category_assignment" USING btree ("canonical_category_id");--> statement-breakpoint
CREATE INDEX "market_category_assignment_provider_category_idx" ON "core"."market_category_assignment" USING btree ("provider_category_id");--> statement-breakpoint
CREATE UNIQUE INDEX "provider_category_1h_provider_group_category_bucket_uq" ON "agg"."provider_category_1h" USING btree ("provider_code","group_by","category_code","bucket_ts");--> statement-breakpoint
CREATE INDEX "provider_category_1h_provider_group_bucket_idx" ON "agg"."provider_category_1h" USING btree ("provider_code","group_by","bucket_ts");
