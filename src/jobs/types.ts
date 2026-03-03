import type { ProviderCode } from "../types/domain.js";
import type { JobName } from "./names.js";

export interface BaseJobPayload {
  requestId?: string;
}

export interface ScopeRebuildPayload extends BaseJobPayload {
  providerCode?: ProviderCode;
}

export interface IngestPayload extends BaseJobPayload {
  providerCode?: ProviderCode;
  scopeStatus?: "active" | "closed" | "all";
  mode?: "topN_live" | "full_catalog";
}

export interface AnalyticsRollupPayload extends BaseJobPayload {
  providerCode?: ProviderCode;
  lookbackHours?: number;
}

export interface CategoryAssignPayload extends BaseJobPayload {
  providerCode?: ProviderCode;
  target?: "scope" | "all";
  maxMarkets?: number;
}

export interface RelinkPayload extends BaseJobPayload {
  providerCode?: ProviderCode;
  maxMarkets?: number | null;
}

export type JobPayload = ScopeRebuildPayload | IngestPayload | AnalyticsRollupPayload | CategoryAssignPayload | RelinkPayload;

export type JobHandler = (payload: JobPayload) => Promise<void>;

export type JobHandlerMap = Partial<Record<JobName, JobHandler>>;
