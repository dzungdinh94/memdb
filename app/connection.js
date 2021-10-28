const P = require("bluebird");
const Logger = require("memdb-logger");
const consts = require("./consts");
const Collection = require("./collection");
const util = require("util");
const utils = require("./utils");

export class Connection {
  constructor(opts) {
    opts = opts || {};

    this._id = opts._id;
    this.shard = opts.shard;

    this.config = opts.config || {};
    this.collections = {};

    this.lockedKeys = utils.forceHashMap();

    this.logger = Logger.getLogger(
      "memdb",
      __filename,
      "shard:" + this.shard._id
    );
    consts.collMethods.forEach(function (method) {
      this[method] = (name) => {
        const collection = this.getCollection(name);
        // remove 'name' arg
        const args = [].slice.call(arguments, 1);

        this.logger.debug("[conn:%s] %s.%s(%j)", this._id, name, method, args);
        return collection[method].apply(collection, args);
      };
    });
  }

  close = () => {
    if (this.isDirty()) {
      this.rollback();
    }
    for (const name in this.collections) {
      this.collections[name].close();
    }
  };

  commit = () => {
    const self = this;
    return P.each(Object.keys(this.collections), function (name) {
      const collection = self.collections[name];
      return collection.commitIndex();
    })
      .then(function () {
        return self.shard.commit(self._id, Object.keys(self.lockedKeys));
      })
      .then(function () {
        self.lockedKeys = {};

        self.logger.debug("[conn:%s] commited", self._id);
        return true;
      });
  };

  // sync method
  rollback = () => {
    const self = this;
    Object.keys(this.collections).forEach(function (name) {
      self.collections[name].rollbackIndex();
    });

    this.shard.rollback(this._id, Object.keys(this.lockedKeys));
    this.lockedKeys = {};

    this.logger.debug("[conn:%s] rolledback", this._id);
    return true;
  };

  flushBackend = () => {
    return this.shard.flushBackend(this._id);
  };

  // for internal use
  $unload = (key) => {
    return this.shard.$unload(key);
  };
  // for internal use
  $findReadOnly = (key, fields) => {
    return this.shard.find(null, key, fields);
  };

  getCollection = (name, isIndex) => {
    if (!isIndex && name && name.indexOf("index.") === 0) {
      throw new Error('Collection name can not begin with "index."');
    }

    const self = this;
    if (!this.collections[name]) {
      const collection = new Collection({
        name: name,
        shard: this.shard,
        conn: this,
        config: this.config.collections[name] || {},
      });

      collection.on("lock", function (id) {
        const key = name + "$" + id;
        self.lockedKeys[key] = true;
      });

      this.collections[name] = collection;
    }
    return this.collections[name];
  };

  isDirty = () => {
    return Object.keys(this.lockedKeys).length > 0;
  };
}

export default Connection;
