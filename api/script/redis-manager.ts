// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import Redis, { RedisOptions } from "ioredis";

export const DEPLOYMENT_SUCCEEDED = "DeploymentSucceeded";
export const DEPLOYMENT_FAILED = "DeploymentFailed";
export const ACTIVE = "Active";
export const DOWNLOADED = "Downloaded";

export interface CacheableResponse {
  statusCode: number;
  body: any;
}

export interface DeploymentMetrics {
  [labelStatus: string]: number;
}

export module Utilities {
  export function isValidDeploymentStatus(status: string): boolean {
    return status === DEPLOYMENT_SUCCEEDED || status === DEPLOYMENT_FAILED || status === DOWNLOADED;
  }

  export function getLabelStatusField(label: string, status: string): string {
    return isValidDeploymentStatus(status) ? `${label}:${status}` : null;
  }

  export function getLabelActiveCountField(label: string): string {
    return label ? `${label}:${ACTIVE}` : null;
  }

  export function getDeploymentKeyHash(deploymentKey: string): string {
    return `deploymentKey:${deploymentKey}`;
  }

  export function getDeploymentKeyLabelsHash(deploymentKey: string): string {
    return `deploymentKeyLabels:${deploymentKey}`;
  }

  export function getDeploymentKeyClientsHash(deploymentKey: string): string {
    return `deploymentKeyClients:${deploymentKey}`;
  }
}

export class RedisManager {
  private static DEFAULT_EXPIRY = 3600; // one hour
  private static METRICS_DB = 1;

  private _opsClient: Redis;
  private _metricsClient: Redis;
  private _isEnabled: boolean;

  constructor() {
    if (process.env.REDIS_HOST && process.env.REDIS_PORT) {
      // const redisConfig: RedisOptions = {
      //   host: process.env.REDIS_HOST,
      //   port: parseInt(process.env.REDIS_PORT),
      //   password: process.env.REDIS_KEY,
      //   tls: { rejectUnauthorized: true },
      // };

      this._opsClient = new Redis();
      this._metricsClient = new Redis({ db: RedisManager.METRICS_DB });

      this._opsClient.on("error", console.error);
      this._metricsClient.on("error", console.error);

      this._isEnabled = true;
    } else {
      console.warn("No REDIS_HOST or REDIS_PORT environment variable configured.");
      this._isEnabled = false;
    }
  }

  get isEnabled(): boolean {
    return this._isEnabled;
  }

  async checkHealth(): Promise<void> {
    if (!this.isEnabled) throw new Error("Redis manager is not enabled");
    await Promise.all([this._opsClient.ping(), this._metricsClient.ping()]);
  }

  async getCachedResponse(expiryKey: string, url: string): Promise<CacheableResponse | null> {
    if (!this.isEnabled) return null;

    const serialized = await this._opsClient.hget(expiryKey, url);
    return serialized ? JSON.parse(serialized) : null;
  }

  async setCachedResponse(expiryKey: string, url: string, response: CacheableResponse): Promise<void> {
    if (!this.isEnabled) return;

    const serialized = JSON.stringify(response);
    const isNewKey = !(await this._opsClient.exists(expiryKey));
    await this._opsClient.hset(expiryKey, url, serialized);
    if (isNewKey) {
      await this._opsClient.expire(expiryKey, RedisManager.DEFAULT_EXPIRY);
    }
  }

  async incrementLabelStatusCount(deploymentKey: string, label: string, status: string): Promise<void> {
    if (!this.isEnabled) return;

    const hash = Utilities.getDeploymentKeyLabelsHash(deploymentKey);
    const field = Utilities.getLabelStatusField(label, status);
    if (field) await this._metricsClient.hincrby(hash, field, 1);
  }

  async clearMetricsForDeploymentKey(deploymentKey: string): Promise<void> {
    if (!this.isEnabled) return;

    await this._metricsClient.del(
      Utilities.getDeploymentKeyLabelsHash(deploymentKey),
      Utilities.getDeploymentKeyClientsHash(deploymentKey)
    );
  }

  async getMetricsWithDeploymentKey(deploymentKey: string): Promise<DeploymentMetrics | null> {
    if (!this.isEnabled) return null;

    const rawMetrics = await this._metricsClient.hgetall(Utilities.getDeploymentKeyLabelsHash(deploymentKey));
    const parsed: DeploymentMetrics = {};

    for (const key in rawMetrics) {
      parsed[key] = parseInt(rawMetrics[key], 10);
    }

    return parsed;
  }

  async recordUpdate(
    currentDeploymentKey: string,
    currentLabel: string,
    previousDeploymentKey?: string,
    previousLabel?: string
  ): Promise<void> {
    if (!this.isEnabled) return;

    const multi = this._metricsClient.multi();

    const currentHash = Utilities.getDeploymentKeyLabelsHash(currentDeploymentKey);
    multi.hincrby(currentHash, Utilities.getLabelActiveCountField(currentLabel), 1);
    multi.hincrby(currentHash, Utilities.getLabelStatusField(currentLabel, DEPLOYMENT_SUCCEEDED), 1);

    if (previousDeploymentKey && previousLabel) {
      const previousHash = Utilities.getDeploymentKeyLabelsHash(previousDeploymentKey);
      multi.hincrby(previousHash, Utilities.getLabelActiveCountField(previousLabel), -1);
    }

    await multi.exec();
  }

  async removeDeploymentKeyClientActiveLabel(deploymentKey: string, clientUniqueId: string): Promise<void> {
    if (!this.isEnabled) return;

    await this._metricsClient.hdel(Utilities.getDeploymentKeyClientsHash(deploymentKey), clientUniqueId);
  }

  async invalidateCache(expiryKey: string): Promise<void> {
    if (!this.isEnabled) return;

    await this._opsClient.del(expiryKey);
  }

  async close(): Promise<void> {
    if (this._opsClient) await this._opsClient.quit();
    if (this._metricsClient) await this._metricsClient.quit();
  }

  /* deprecated */
  async getCurrentActiveLabel(deploymentKey: string, clientUniqueId: string): Promise<string | null> {
    if (!this.isEnabled) return null;

    return this._metricsClient.hget(Utilities.getDeploymentKeyClientsHash(deploymentKey), clientUniqueId);
  }

  /* deprecated */
  async updateActiveAppForClient(deploymentKey: string, clientUniqueId: string, toLabel: string, fromLabel?: string): Promise<void> {
    if (!this.isEnabled) return;

    const multi = this._metricsClient.multi();
    const deploymentKeyClientsHash = Utilities.getDeploymentKeyClientsHash(deploymentKey);
    const deploymentKeyLabelsHash = Utilities.getDeploymentKeyLabelsHash(deploymentKey);

    multi.hset(deploymentKeyClientsHash, clientUniqueId, toLabel);
    multi.hincrby(deploymentKeyLabelsHash, Utilities.getLabelActiveCountField(toLabel), 1);

    if (fromLabel) {
      multi.hincrby(deploymentKeyLabelsHash, Utilities.getLabelActiveCountField(fromLabel), -1);
    }

    await multi.exec();
  }
}
