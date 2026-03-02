ALTER TABLE "raw"."price_point_5m" RENAME TO "price_point";

ALTER TABLE "raw"."price_point"
  RENAME CONSTRAINT "price_point_5m_instrument_id_instrument_id_fk"
  TO "price_point_instrument_id_instrument_id_fk";

ALTER INDEX "raw"."price_point_5m_instrument_ts_uq" RENAME TO "price_point_instrument_ts_uq";
ALTER INDEX "raw"."price_point_5m_ts_idx" RENAME TO "price_point_ts_idx";

ALTER SEQUENCE "raw"."price_point_5m_id_seq" RENAME TO "price_point_id_seq";
