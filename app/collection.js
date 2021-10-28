const P = require("bluebird");
const Logger = require("memdb-logger");
const util = require("util");
const utils = require("./utils");
const EventEmitter = require("events").EventEmitter;

const DEFAULT_MAX_COLLISION = 10000;

export class Collection extends EventEmitter {
  constructor(opts) {
    opts = opts || {};

    this.name = opts.name;
    this._checkName(this.name);

    this.shard = opts.shard;
    this.conn = opts.conn;
    this.config = opts.config || {};
    this.config.maxCollision =
      this.config.maxCollision || DEFAULT_MAX_COLLISION;

    // {indexKey : {indexValue : {id1 : 1, id2 : -1}}}
    this.changedIndexes = utils.forceHashMap();

    this.pendingIndexTasks = utils.forceHashMap(); //{id, [Promise]}

    this.updateIndexEvent = "updateIndex$" + this.name + "$" + this.conn._id;
    this.shard.on(this.updateIndexEvent, this.onUpdateIndex.bind(this));

    this.logger = Logger.getLogger(
      "memdb",
      __filename,
      "shard:" + this.shard._id
    );

    EventEmitter.call(this);
  }

  close = () => {
    this.shard.removeListener(this.updateIndexEvent, this.onUpdateIndex);
  };

  insert = (docs) => {
    if (!Array.isArray(docs)) {
      return this._insertById(docs && docs._id, docs);
    }

    const self = this;
    return P.mapSeries(docs, function (doc) {
      //disable concurrent to avoid race condition
      return self._insertById(doc && doc._id, doc);
    });
  };

  _insertById = (id, doc) => {
    if (!utils.isDict(doc)) {
      throw new Error("doc must be a dictionary");
    }

    if (id === null || id === undefined) {
      id = utils.uuid();
    }
    id = this._checkId(id);
    doc._id = id;

    const self = this;
    return P.try(function () {
      return self.lock(id);
    })
      .then(function () {
        return self.shard.insert(self.conn._id, self._key(id), doc);
      })
      .then(function () {
        return self._finishIndexTasks(id);
      })
      .thenReturn(id);
  };

  find = (query, fields, opts) => {
    if (typeof query === "number" || typeof query === "string") {
      return this.findById(query, fields, opts);
    }

    if (!utils.isDict(query)) {
      throw new Error("invalid query");
    }

    if (query.hasOwnProperty("_id")) {
      return this.findById(query._id, fields, opts).then(function (doc) {
        if (!doc) {
          return [];
        }
        return [doc];
      });
    }

    const keys = Object.keys(query).sort();
    if (keys.length === 0) {
      throw new Error("You must specify query key");
    }

    const indexKey = JSON.stringify(keys);

    const indexConfig = this.config.indexes && this.config.indexes[indexKey];
    if (!indexConfig) {
      throw new Error("No index configured for keys - " + indexKey);
    }

    const valueIgnore = indexConfig.valueIgnore || {};
    const values = keys.map(function (key) {
      const value = query[key];
      if (value === null || value === undefined) {
        throw new Error("query value can not be null or undefined");
      }
      const ignores = valueIgnore[key] || [];
      if (ignores.indexOf(value) !== -1) {
        throw new Error(
          "value " + value + " for key " + key + " is ignored in index"
        );
      }
      return value;
    });
    const indexValue = JSON.stringify(values);

    return this._findByIndex(indexKey, indexValue, fields, opts);
  };

  findOne = (query, fields, opts) => {
    opts = opts || {};
    opts.limit = 1;
    return this.find(query, fields, opts).then(function (docs) {
      if (!Array.isArray(docs)) {
        return docs;
      }
      if (docs.length === 0) {
        return null;
      }
      return docs[0];
    });
  };

  findById = (id, fields, opts) => {
    id = this._checkId(id);

    const self = this;
    return P.try(function () {
      if (opts && opts.readonly) {
        return self.shard.findReadOnly(self.conn._id, self._key(id), fields);
      }

      return P.try(function () {
        if (opts && opts.nolock) {
          return;
        }
        return self.lock(id);
      }).then(function () {
        return self.shard.find(self.conn._id, self._key(id), fields, opts);
      });
    });
  };

  findReadOnly = (query, fields, opts) => {
    opts = opts || {};
    opts.readonly = true;
    return this.find(query, fields, opts);
  };

  findOneReadOnly = (query, fields, opts) => {
    opts = opts || {};
    opts.readonly = true;
    return this.findOne(query, fields, opts);
  };

  findByIdReadOnly = (id, fields, opts) => {
    opts = opts || {};
    opts.readonly = true;
    return this.findById(id, fields, opts);
  };

  count = (query, opts) => {
    opts = opts || {};
    opts.count = true;
    const self = this;
    return P.try(function () {
      return self.find(query, null, opts);
    }).then(function (ret) {
      if (typeof ret === "number") {
        return ret;
      } else if (Array.isArray(ret)) {
        return ret.length;
      } else if (!ret) {
        return 0;
      }
      throw new Error("Unexpected find result - " + ret);
    });
  };

  _findByIndex = (indexKey, indexValue, fields, opts) => {
    opts = opts || {};
    const self = this;

    const indexCollection = this.conn.getCollection(
      this._indexCollectionName(indexKey),
      true
    );

    const nolock = opts.nolock;

    return P.try(function () {
      opts.nolock = true; // force not using lock
      return indexCollection.findById(indexValue, "format ids", opts);
    }).then(function (doc) {
      opts.nolock = nolock; // restore param

      const ids = utils.forceHashMap();

      if (doc) {
        doc.ids.forEach(function (id) {
          ids[id] = 1;
        });
      }

      const changedIds =
        (self.changedIndexes[indexKey] &&
          self.changedIndexes[indexKey][indexValue]) ||
        {};
      for (const id in changedIds) {
        if (changedIds[id] === 1) {
          ids[id] = 1;
        } else {
          delete ids[id];
        }
      }

      ids = Object.keys(ids);
      if (opts && opts.count) {
        // return count only
        return ids.length;
      }
      if (opts && opts.limit) {
        ids = ids.slice(0, opts.limit);
      }

      const docs = [];
      ids.sort(); // keep id in order, avoid deadlock
      return P.each(ids, function (id) {
        return self.findById(id, fields, opts).then(function (doc) {
          // WARN: This is possible that doc is null due to index collection is not locked
          if (!!doc) {
            docs.push(doc);
          }
        });
      }).thenReturn(docs);
    });
  };

  update = (query, modifier, opts) => {
    opts = opts || {};
    const self = this;

    return P.try(function () {
      return self.find(query, "_id");
    }).then(function (ret) {
      if (!ret || ret.length === 0) {
        if (!opts.upsert) {
          return 0;
        }
        // upsert
        if (typeof query === "string" || typeof query === "number") {
          query = { _id: query };
        }
        return self
          .insert(query)
          .then(function (id) {
            return self._updateById(id, modifier, opts);
          })
          .thenReturn(1);
      }

      if (!Array.isArray(ret)) {
        return self._updateById(ret._id, modifier, opts).thenReturn(1);
      }
      return P.each(ret, function (doc) {
        return self._updateById(doc._id, modifier, opts);
      }).thenReturn(ret.length);
    });
  };

  _updateById = (id, modifier, opts) => {
    id = this._checkId(id);

    const self = this;
    return P.try(function () {
      return self.shard.update(self.conn._id, self._key(id), modifier, opts);
    }).then(function () {
      return self._finishIndexTasks(id);
    });
  };

  remove = (query, opts) => {
    const self = this;
    return P.try(function () {
      return self.find(query, "_id");
    }).then(function (ret) {
      if (!ret || ret.length === 0) {
        return 0;
      }
      if (!Array.isArray(ret)) {
        return self._removeById(ret._id, opts).thenReturn(1);
      }
      return P.each(ret, function (doc) {
        return self._removeById(doc._id, opts);
      }).thenReturn(ret.length);
    });
  };

  _removeById = (id, opts) => {
    id = this._checkId(id);

    const self = this;
    return P.try(function () {
      return self.shard.remove(self.conn._id, self._key(id), opts);
    }).then(function () {
      return self._finishIndexTasks(id);
    });
  };

  lock = (id) => {
    id = this._checkId(id);
    if (this.shard.isLocked(this.conn._id, this._key(id))) {
      return;
    }

    const self = this;
    return this.shard.lock(this.conn._id, this._key(id)).then(function (ret) {
      self.emit("lock", id);
      return ret;
    });
  };

  onUpdateIndex = (id, indexKey, oldValue, newValue) => {
    this.logger.debug(
      "onUpdateIndex(%s, %s, %s, %s)",
      id,
      indexKey,
      oldValue,
      newValue
    );

    const self = this;
    const promise = P.try(function () {
      const config = self.config.indexes[indexKey];
      if (!config) {
        throw new Error("index - " + indexKey + " not configured");
      }
      if (!self.changedIndexes[indexKey]) {
        self.changedIndexes[indexKey] = utils.forceHashMap();
      }

      const changedIndex = self.changedIndexes[indexKey];

      if (oldValue !== null) {
        if (!changedIndex[oldValue]) {
          changedIndex[oldValue] = utils.forceHashMap();
        }
        if (changedIndex[oldValue][id] === 1) {
          delete changedIndex[oldValue][id];
        } else {
          changedIndex[oldValue][id] = -1;
        }
      }
      if (newValue !== null) {
        if (!changedIndex[newValue]) {
          changedIndex[newValue] = utils.forceHashMap();
        }
        if (changedIndex[newValue][id] === -1) {
          delete changedIndex[oldValue][id];
        } else {
          changedIndex[newValue][id] = 1;
        }
      }

      if (!config.unique) {
        return;
      }

      return P.try(function () {
        if (oldValue !== null) {
          return self
            .commitOneIndex(indexKey, oldValue, changedIndex[oldValue], config)
            .then(function () {
              delete changedIndex[oldValue];
            });
        }
      }).then(function () {
        if (newValue !== null) {
          return self
            .commitOneIndex(indexKey, newValue, changedIndex[newValue], config)
            .then(function () {
              delete changedIndex[newValue];
            });
        }
      });
    });

    if (!this.pendingIndexTasks[id]) {
      this.pendingIndexTasks[id] = [];
    }
    this.pendingIndexTasks[id].push(promise);
  };

  commitIndex = () => {
    const self = this;

    // must update in sorted order to avoid dead lock
    return P.each(Object.keys(this.changedIndexes).sort(), function (indexKey) {
      const changedIndex = self.changedIndexes[indexKey];
      const config = self.config.indexes[indexKey];

      return P.each(Object.keys(changedIndex).sort(), function (indexValue) {
        const changedIds = changedIndex[indexValue];

        return self.commitOneIndex(indexKey, indexValue, changedIds, config);
      });
    }).then(function () {
      self.changedIndexes = utils.forceHashMap();
    });
  };

  rollbackIndex = () => {
    this.changedIndexes = utils.forceHashMap();
  };

  // indexKey: json encoded sorted fields array
  // indexValue: json encoded sorted fields value
  commitOneIndex = (indexKey, indexValue, changedIds, config) => {
    const indexCollection = this.conn.getCollection(
      this._indexCollectionName(indexKey),
      true
    );

    const modifier = { $pushAll: { ids: [] }, $pullAll: { ids: [] } };

    const countDelta = 0;
    for (const id in changedIds) {
      if (changedIds[id] === 1) {
        modifier.$pushAll.ids.push(id);
        countDelta++;
      } else {
        modifier.$pullAll.ids.push(id);
        countDelta--;
      }
    }

    const self = this;
    return P.try(function () {
      return indexCollection.find(indexValue, "ids");
    }).then(function (ret) {
      const oldCount = ret ? ret.ids.length : 0;

      const newCount = oldCount + countDelta;
      if (config.unique && newCount > 1) {
        throw new Error(
          "duplicate value - " + indexValue + " for unique index - " + indexKey
        );
      }
      if (newCount > config.maxCollision) {
        throw new Error(
          "too many documents have value - " +
            indexValue +
            " for index - " +
            indexKey
        );
      }

      if (newCount > 0) {
        return indexCollection.update(indexValue, modifier, { upsert: true });
      } else if (newCount === 0) {
        return indexCollection.remove(indexValue);
      } else {
        throw new Error(
          util.format(
            "index corrupted: %s %s, please rebuild index",
            self.name,
            indexKey
          )
        );
      }
    });
  };

  _finishIndexTasks = (id) => {
    if (!this.pendingIndexTasks[id]) {
      return;
    }
    // Save domain
    const d = process.domain;
    const self = this;
    return P.each(self.pendingIndexTasks[id], function (promise) {
      return promise;
    }).finally(function () {
      delete self.pendingIndexTasks[id];
      // Restore domain
      process.domain = d;
    });
  };

  // 'index.name.key1.key2'
  _indexCollectionName = (indexKey) => {
    const keys = JSON.parse(indexKey).map(function (key) {
      return utils.escapeField(key);
    });
    return "index." + utils.escapeField(this.name) + "." + keys.join(".");
  };

  _key = (id) => {
    return this.name + "$" + id;
  };

  _checkId = (id) => {
    if (typeof id === "string") {
      return id;
    } else if (typeof id === "number") {
      return id.toString();
    }
    throw new Error("id must be number or string");
  };

  //http://docs.mongodb.org/manual/reference/limits/#Restriction-on-Collection-Names
  _checkName = (name) => {
    if (!name) {
      throw new Error("Collection name can not empty");
    }
    if (typeof name !== "string") {
      throw new Error("Collection name must be string");
    }
    if (name.indexOf("$") !== -1) {
      throw new Error('Collection name can not contain "$"');
    }
    if (name.indexOf("system.") === 0) {
      throw new Error('Collection name can not begin with "system."');
    }
  };
}
export default Collection;
