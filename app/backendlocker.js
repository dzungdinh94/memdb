const P = require("bluebird");
const Logger = require("memdb-logger");
const util = require("util");
const redis = P.promisifyAll(require("redis"));

export class BackendLocker {
  constructor(opts) {
    opts = opts || {};

    this.shardId = opts.shardId;
    this.config = {
      host: opts.host || "127.0.0.1",
      port: opts.port || 6379,
      db: opts.db || 0,
      options: opts.options || {},
      prefix: opts.prefix || "bl$",
      heartbeatPrefix: opts.heartbeatPrefix || "hb$",
      heartbeatTimeout: opts.heartbeatTimeout,
      heartbeatInterval: opts.heartbeatInterval,
    };

    this.client = null;
    this.heartbeatInterval = null;

    this.logger = Logger.getLogger(
      "memdb",
      __filename,
      "shard:" + this.shardId
    );
  }

  start = () => {
    return P.bind(this)
      .then(function () {
        this.client = redis.createClient(this.config.port, this.config.host, {
          retry_max_delay: 10 * 1000,
          enable_offline_queue: true,
        });
        const self = this;
        this.client.on("error", function (err) {
          self.logger.error(err.stack);
        });
        return this.client.selectAsync(this.config.db);
      })
      .then(function () {
        if (this.shardId) {
          return this.isAlive().then(function (ret) {
            if (ret) {
              throw new Error("Current shard is running in some other process");
            }
          });
        }
      })
      .then(function () {
        if (this.shardId && this.config.heartbeatInterval > 0) {
          this.heartbeatInterval = setInterval(
            this.heartbeat.bind(this),
            this.config.heartbeatInterval
          );
          return this.heartbeat();
        }
      })
      .then(function () {
        this.logger.info(
          "backendLocker started %s:%s:%s",
          this.config.host,
          this.config.port,
          this.config.db
        );
      });
  };

  stop = () => {
    return P.bind(this)
      .then(function () {
        clearInterval(this.heartbeatInterval);
        return this.clearHeartbeat();
      })
      .then(function () {
        return this.client.quitAsync();
      })
      .then(function () {
        this.logger.info("backendLocker stoped");
      });
  };

  tryLock = (docId, shardId) => {
    this.logger.debug("tryLock %s", docId);

    const self = this;
    return this.client
      .setnxAsync(this._docKey(docId), shardId || this.shardId)
      .then(function (ret) {
        if (ret === 1) {
          self.logger.debug("locked %s", docId);
          return true;
        } else {
          return false;
        }
      });
  };

  getHolderId = (docId) => {
    return this.client.getAsync(this._docKey(docId));
  };

  isHeld = (docId, shardId) => {
    const self = this;
    return this.getHolderId(docId).then(function (ret) {
      return ret === (shardId || self.shardId);
    });
  };

  // concurrency safe between shards
  // not concurrency safe in same shard
  unlock = (docId) => {
    this.logger.debug("unlock %s", docId);

    const self = this;
    return this.isHeld(docId).then(function (held) {
      if (held) {
        return self.client.delAsync(self._docKey(docId));
      }
    });
  };

  heartbeat = () => {
    const timeout = Math.floor(this.config.heartbeatTimeout / 1000);
    if (timeout <= 0) {
      timeout = 1;
    }

    const self = this;
    return this.client
      .setexAsync(this._heartbeatKey(this.shardId), timeout, 1)
      .then(function () {
        self.logger.debug("heartbeat");
      })
      .catch(function (err) {
        self.logger.error(err.stack);
      });
  };

  clearHeartbeat = () => {
    return this.client.delAsync(this._heartbeatKey(this.shardId));
  };

  isAlive = (shardId) => {
    return this.client
      .existsAsync(this._heartbeatKey(shardId || this.shardId))
      .then(function (ret) {
        return !!ret;
      });
  };

  getActiveShards = () => {
    const prefix = this.config.heartbeatPrefix;
    return this.client.keysAsync(prefix + "*").then(function (keys) {
      return keys.map(function (key) {
        return key.slice(prefix.length);
      });
    });
  };

  _docKey = (docId) => {
    return this.config.prefix + docId;
  };

  _heartbeatKey = (shardId) => {
    return this.config.heartbeatPrefix + shardId;
  };
}

export default BackendLocker;
