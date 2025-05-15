import Redis, { RedisOptions } from "ioredis";
import * as Q from "q";

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
    this._opsClient = new Redis();
    this._metricsClient = new Redis({ db: RedisManager.METRICS_DB });

    this._opsClient.on("error", console.error);
    this._metricsClient.on("error", console.error);

    this._isEnabled = true;
  }

  get isEnabled(): boolean {
    return this._isEnabled;
  }

  checkHealth(): Q.Promise<void> {
    if (!this.isEnabled) return Q.reject(new Error("Redis manager is not enabled"));

    return Q.all([Q(this._opsClient.ping()), Q(this._metricsClient.ping())]).then(() => {});
  }

  getCachedResponse(expiryKey: string, url: string): Q.Promise<CacheableResponse | null> {
    if (!this.isEnabled) return Q.resolve(null);

    return Q.Promise((resolve, reject) => {
      this._opsClient
        .hget(expiryKey, url)
        .then((serialized) => {
          if (serialized) {
            try {
              resolve(JSON.parse(serialized));
            } catch (err) {
              reject(err);
            }
          } else {
            resolve(null);
          }
        })
        .catch(reject);
    });
  }

  setCachedResponse(expiryKey: string, url: string, response: CacheableResponse): Q.Promise<void> {
    if (!this.isEnabled) return Q.resolve();

    return Q.Promise(async (resolve, reject) => {
      try {
        const serialized = JSON.stringify(response);
        const isNewKey = !(await this._opsClient.exists(expiryKey));
        await this._opsClient.hset(expiryKey, url, serialized);
        if (isNewKey) {
          await this._opsClient.expire(expiryKey, RedisManager.DEFAULT_EXPIRY);
        }
        resolve();
      } catch (err) {
        reject(err);
      }
    });
  }

  incrementLabelStatusCount(deploymentKey: string, label: string, status: string): Q.Promise<void> {
    if (!this.isEnabled) return Q.resolve();

    const hash = Utilities.getDeploymentKeyLabelsHash(deploymentKey);
    const field = Utilities.getLabelStatusField(label, status);
    if (!field) return Q.resolve();

    return Q(this._metricsClient.hincrby(hash, field, 1)).then(() => {});
  }

  clearMetricsForDeploymentKey(deploymentKey: string): Q.Promise<void> {
    if (!this.isEnabled) return Q.resolve();

    return Q(
      this._metricsClient.del(
        Utilities.getDeploymentKeyLabelsHash(deploymentKey),
        Utilities.getDeploymentKeyClientsHash(deploymentKey)
      )
    ).then(() => {});
  }

  getMetricsWithDeploymentKey(deploymentKey: string): Q.Promise<DeploymentMetrics | null> {
    if (!this.isEnabled) return Q.resolve(null);

    return Q.Promise((resolve, reject) => {
      this._metricsClient
        .hgetall(Utilities.getDeploymentKeyLabelsHash(deploymentKey))
        .then((rawMetrics) => {
          const parsed: DeploymentMetrics = {};
          for (const key in rawMetrics) {
            parsed[key] = parseInt(rawMetrics[key], 10);
          }
          resolve(parsed);
        })
        .catch(reject);
    });
  }

  recordUpdate(
    currentDeploymentKey: string,
    currentLabel: string,
    previousDeploymentKey?: string,
    previousLabel?: string
  ): Q.Promise<void> {
    if (!this.isEnabled) return Q.resolve();

    const multi = this._metricsClient.multi();

    const currentHash = Utilities.getDeploymentKeyLabelsHash(currentDeploymentKey);
    multi.hincrby(currentHash, Utilities.getLabelActiveCountField(currentLabel), 1);
    multi.hincrby(currentHash, Utilities.getLabelStatusField(currentLabel, DEPLOYMENT_SUCCEEDED), 1);

    if (previousDeploymentKey && previousLabel) {
      const previousHash = Utilities.getDeploymentKeyLabelsHash(previousDeploymentKey);
      multi.hincrby(previousHash, Utilities.getLabelActiveCountField(previousLabel), -1);
    }

    return Q.Promise((resolve, reject) => {
      multi.exec((err, results) => {
        if (err) return reject(err);
        resolve(results);
      });
    }).then(() => {});
  }

  removeDeploymentKeyClientActiveLabel(deploymentKey: string, clientUniqueId: string): Q.Promise<void> {
    if (!this.isEnabled) return Q.resolve();

    return Q(this._metricsClient.hdel(Utilities.getDeploymentKeyClientsHash(deploymentKey), clientUniqueId)).then(() => {});
  }

  invalidateCache(expiryKey: string): Q.Promise<void> {
    if (!this.isEnabled) return Q.resolve();

    return Q(this._opsClient.del(expiryKey)).then(() => {});
  }

  close(): Q.Promise<void> {
    return Q.all([
      this._opsClient ? Q(this._opsClient.quit()) : Q.resolve(),
      this._metricsClient ? Q(this._metricsClient.quit()) : Q.resolve(),
    ]).then(() => {});
  }

  /* deprecated */
  getCurrentActiveLabel(deploymentKey: string, clientUniqueId: string): Q.Promise<string | null> {
    if (!this.isEnabled) return Q.resolve(null);

    return Q(this._metricsClient.hget(Utilities.getDeploymentKeyClientsHash(deploymentKey), clientUniqueId));
  }

  /* deprecated */
  updateActiveAppForClient(deploymentKey: string, clientUniqueId: string, toLabel: string, fromLabel?: string): Q.Promise<void> {
    if (!this.isEnabled) return Q.resolve();

    const multi = this._metricsClient.multi();
    const deploymentKeyClientsHash = Utilities.getDeploymentKeyClientsHash(deploymentKey);
    const deploymentKeyLabelsHash = Utilities.getDeploymentKeyLabelsHash(deploymentKey);

    multi.hset(deploymentKeyClientsHash, clientUniqueId, toLabel);
    multi.hincrby(deploymentKeyLabelsHash, Utilities.getLabelActiveCountField(toLabel), 1);

    if (fromLabel) {
      multi.hincrby(deploymentKeyLabelsHash, Utilities.getLabelActiveCountField(fromLabel), -1);
    }

    return Q.Promise((resolve, reject) => {
      multi.exec((err, results) => {
        if (err) return reject(err);
        resolve(results);
      });
    }).then(() => {});
  }
}
