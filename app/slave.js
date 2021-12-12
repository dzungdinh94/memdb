"use strict";

import P from "bluebird";
import Logger from "memdb-logger";
import utils from "./utils";
const redis = P.promisifyAll(require("redis"));
export class Slave {
  constructor(opts) {
    opts = opts || {};

    this.shardId = opts.shardId;

    this.config = {
      host: opts.host || "127.0.0.1",
      port: opts.port || 6379,
      db: opts.db || 0,
      options: opts.options || {},
    };

    this.client = null;
    this.logger = Logger.getLogger(
      "memdb",
      __filename,
      "shard:" + this.shardId
    );
  }

  start = () => {
    return P.bind(this)
      .then(function () {
        this.client = redis.createClient(
          this.config.port,
          this.config.host,
          this.config.options
        );
        const self = this;
        this.client.on("error", function (err) {
          self.logger.error(err.stack);
        });
        return this.client.selectAsync(this.config.db);
      })
      .then(function () {
        this.logger.info(
          "slave started %s:%s:%s",
          this.config.host,
          this.config.port,
          this.config.db
        );
      });
  };

  stop = () => {
    return P.bind(this)
      .then(function () {
        return this.client.quitAsync();
      })
      .then(function () {
        this.logger.info("slave stoped");
      });
  };

  set =  (key, doc) => {
    this.logger.debug("slave set %s", key);
    return this.client.setAsync(this._redisKey(key), JSON.stringify(doc));
  };

  del =  (key) => {
    this.logger.debug("slave del %s", key);
    return this.client.delAsync(this._redisKey(key));
  };

  // docs - {key : doc}
  setMulti =  (docs) => {
    this.logger.debug("slave setMulti");

    const multi = this.client.multi();
    for (const key in docs) {
      const doc = docs[key];
      multi = multi.set(this._redisKey(key), JSON.stringify(doc));
    }

    return multi.execAsync();
  };

  // returns - {key : doc}
  getMulti =  (keys) => {
    this.logger.debug("slave getMulti");

    const self = this;
    const multi = this.client.multi();
    keys.forEach(function (key) {
      multi = multi.get(self._redisKey(key));
    });

    return multi.execAsync().then(function (results) {
      const docs = {};
      for (const i in keys) {
        const key = keys[i];
        if (!!results[i]) {
          docs[key] = JSON.parse(results[i]);
        }
      }
      return docs;
    });
  };

  getAllKeys = () => {
    this.logger.debug("slave getAllKeys");

    return P.bind(this)
      .then(function () {
        return this.client.keysAsync(this._redisKey("*"));
      })
      .then(function (keys) {
        const self = this;
        return keys.map(function (key) {
          return self._extractKey(key);
        });
      });
  };

  clear = () => {
    this.logger.debug("slave clear");

    return P.bind(this)
      .then(function () {
        return this.client.keysAsync(this._redisKey("*"));
      })
      .then(function (keys) {
        const multi = this.client.multi();
        keys.forEach(function (key) {
          multi = multi.del(key);
        });
        return multi.execAsync();
      });
  };

  _redisKey = (key) => {
    return "bk$" + this.shardId + "$" + key;
  };

  _extractKey = (existKey) => {
    const words = existKey.split("$");
    return words.slice(2, words.length).join("$");
  };
}

export default Slave;
