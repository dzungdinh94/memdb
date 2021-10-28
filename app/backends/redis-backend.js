const P = require("bluebird");
const Logger = require("memdb-logger");
const redis = P.promisifyAll(require("redis"));

export class RedisBackend {
  constructor(opts) {
    opts = opts || {};

    this.config = {
      host: opts.host || "127.0.0.1",
      port: opts.port || 6379,
      db: opts.db || 0,
      options: opts.option || {},
      prefix: opts.prefix || "",
    };
    this.conn = null;

    this.logger = Logger.getLogger(
      "memdb",
      __filename,
      "shard:" + opts.shardId
    );
  }

  start = () => {
    this.conn = redis.createClient(this.config.port, this.config.host, {
      retry_max_delay: 10 * 1000,
    });

    const self = this;
    this.conn.on("error", function (err) {
      self.logger.error(err.stack);
    });

    this.conn.select(this.config.db);

    this.logger.debug(
      "backend redis connected to %s:%s:%s",
      this.config.host,
      this.config.port,
      this.config.db
    );
  };

  stop = () => {
    this.logger.debug("backend redis stop");
    return this.conn.quitAsync();
  };

  get = (name, id) => {
    this.logger.debug("backend redis get(%s, %s)", name, id);

    return P.bind(this)
      .then(function () {
        return this.conn.hmgetAsync(this.config.prefix + name, id);
      })
      .then(function (ret) {
        ret = ret[0];
        return JSON.parse(ret);
      });
  };

  // Return an async iterator with .next(cb) signature
  getAll = (name) => {
    throw new Error("not implemented");
  };

  // delete when doc is null
  set = (name, id, doc) => {
    this.logger.debug("backend redis set(%s, %s)", name, id);

    if (!!doc) {
      return this.conn.hmsetAsync(
        this.config.prefix + name,
        id,
        JSON.stringify(doc)
      );
    } else {
      return this.conn.hdelAsync(this.config.prefix + name, id);
    }
  };

  // items : [{name, id, doc}]
  setMulti = (items) => {
    this.logger.debug("backend redis setMulti");

    const multi = this.conn.multi();

    const self = this;
    items.forEach(function (item) {
      if (!!item.doc) {
        multi = multi.hmset(
          self.config.prefix + item.name,
          item.id,
          JSON.stringify(item.doc)
        );
      } else {
        multi = multi.hdel(self.config.prefix + item.name, item.id);
      }
    });
    return multi.execAsync();
  };

  // drop table or database
  drop = (name) => {
    this.logger.debug("backend redis drop %s", name);

    if (!!name) {
      throw new Error("not implemented");
      //this.conn.delAsync(this.config.prefix + name);
    } else {
      this.conn.flushdbAsync();
    }
  };

  getCollectionNames = () => {
    throw new Error("not implemented");
  };
}

export default RedisBackend;
