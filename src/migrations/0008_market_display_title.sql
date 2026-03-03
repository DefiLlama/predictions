ALTER TABLE "core"."market" ADD COLUMN "display_title" text;
--> statement-breakpoint
WITH derived AS (
  SELECT
    m.id,
    CASE
      WHEN p.code = 'polymarket' THEN
        COALESCE(
          NULLIF(TRIM(m.raw_json->>'groupItemTitle'), ''),
          NULLIF(TRIM(m.title), ''),
          NULLIF(TRIM(m.raw_json->>'question'), '')
        )
      WHEN p.code = 'kalshi' THEN
        COALESCE(
          NULLIF(TRIM(m.raw_json->>'yes_sub_title'), ''),
          NULLIF(TRIM(m.raw_json->>'yes_title'), ''),
          NULLIF(TRIM(m.raw_json->>'subtitle'), ''),
          NULLIF(TRIM(m.raw_json->>'sub_title'), ''),
          NULLIF(TRIM(m.title), ''),
          NULLIF(TRIM(m.raw_json->>'title'), '')
        )
      ELSE
        COALESCE(
          NULLIF(TRIM(m.title), ''),
          NULLIF(TRIM(m.raw_json->>'title'), '')
        )
    END AS display_title
  FROM "core"."market" m
  JOIN "core"."platform" p
    ON p.id = m.platform_id
)
UPDATE "core"."market" m
SET display_title = d.display_title
FROM derived d
WHERE m.id = d.id
  AND m.display_title IS DISTINCT FROM d.display_title;
