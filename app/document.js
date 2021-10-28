const P = require("bluebird");
const util = require("util");
const utils = require("./utils");
const AsyncLock = require("async-lock");
const EventEmitter = require("events").EventEmitter;
const modifier = require("./modifier");
const logger = require("memdb-logger").getLogger("memdb", __filename);

const DEFAULT_LOCK_TIMEOUT = 10 * 1000;
export class Document extends EventEmitter {
  constructor(opts) {
    opts = opts || {};

    if (!opts.hasOwnProperty("_id")) {
      throw new Error("_id is not specified");
    }
    this._id = opts._id;

    const doc = opts.doc || null;
    if (typeof doc !== "object") {
      throw new Error("doc must be object");
    }
    if (!!doc) {
      doc._id = this._id;
    }

    this.commited = doc;
    this.changed = undefined; // undefined means no change, while null means removed
    this.connId = null; // Connection that hold the document lock

    this.locker = opts.locker;
    this.lockKey = opts.lockKey;
    if (!this.locker) {
      this.locker = new AsyncLock({
        Promise: P,
        timeout: opts.lockTimeout || DEFAULT_LOCK_TIMEOUT,
      });
      this.lockKey = "";
    }

    this.releaseCallback = null;

    this.indexes = opts.indexes || {};

    this.savedIndexValues = {}; //{indexKey : indexValue}

    EventEmitter.call(this);
  }

  find = (connId, fields) => {
    const doc = this.isLocked(connId) ? this._getChanged() : this.commited;

    if (doc === null) {
      return null;
    }

    if (!fields) {
      return doc;
    }

    const includeFields = [],
      excludeFields = [];

    if (typeof fields === "string") {
      includeFields = fields.split(" ");
    } else if (typeof fields === "object") {
      for (const field in fields) {
        if (!!fields[field]) {
          includeFields.push(field);
        } else {
          excludeFields.push(field);
        }
      }
      if (includeFields.length > 0 && excludeFields.length > 0) {
        throw new Error("Can not specify both include and exclude fields");
      }
    }

    const ret = null;
    if (includeFields.length > 0) {
      ret = {};
      includeFields.forEach(function (field) {
        if (doc.hasOwnProperty(field)) {
          ret[field] = doc[field];
        }
      });
      ret._id = this._id;
    } else if (excludeFields.length > 0) {
      ret = {};
      for (const key in doc) {
        ret[key] = doc[key];
      }
      excludeFields.forEach(function (key) {
        delete ret[key];
      });
    } else {
      ret = doc;
    }

    return ret;
  };

  exists = (connId) => {
    return this.isLocked(connId)
      ? this._getChanged() !== null
      : this.commited !== null;
  };

  insert = (connId, doc) => {
    this.modify(connId, "$insert", doc);
  };

  remove = (connId) => {
    this.modify(connId, "$remove");
  };

  update = (connId, modifier, opts) => {
    opts = opts || {};
    if (!modifier) {
      throw new Error("modifier is empty");
    }

    modifier = modifier || {};

    const isModify = false;
    for (const field in modifier) {
      isModify = field[0] === "$";
      break;
    }

    if (!isModify) {
      this.modify(connId, "$replace", modifier);
    } else {
      for (const cmd in modifier) {
        this.modify(connId, cmd, modifier[cmd]);
      }
    }
  };

  modify = (connId, cmd, param) => {
    this.ensureLocked(connId);

    for (const indexKey in this.indexes) {
      if (!this.savedIndexValues.hasOwnProperty(indexKey)) {
        this.savedIndexValues[indexKey] = this._getIndexValue(
          indexKey,
          this.indexes[indexKey]
        );
      }
    }

    const modifyFunc = modifier[cmd];
    if (typeof modifyFunc !== "function") {
      throw new Error("invalid modifier - " + cmd);
    }

    if (this.changed === undefined) {
      //copy on write
      this.changed = utils.clone(this.commited);
    }

    this.changed = modifyFunc(this.changed, param);

    // id is immutable
    if (!!this.changed) {
      this.changed._id = this._id;
    }

    for (indexKey in this.indexes) {
      const value = this._getIndexValue(indexKey, this.indexes[indexKey]);

      if (value !== this.savedIndexValues[indexKey]) {
        logger.trace(
          "%s.updateIndex(%s, %s, %s)",
          this._id,
          indexKey,
          this.savedIndexValues[indexKey],
          value
        );
        this.emit(
          "updateIndex",
          connId,
          indexKey,
          this.savedIndexValues[indexKey],
          value
        );

        this.savedIndexValues[indexKey] = value;
      }
    }

    logger.trace("%s.modify(%s, %j) => %j", this._id, cmd, param, this.changed);
  };

  lock = (connId) => {
    if (connId === null || connId === undefined) {
      throw new Error("connId is null");
    }

    const deferred = P.defer();
    if (this.isLocked(connId)) {
      deferred.resolve();
    } else {
      const self = this;
      this.locker
        .acquire(this.lockKey, function (release) {
          self.connId = connId;
          self.releaseCallback = release;

          self.emit("lock");
          deferred.resolve();
        })
        .catch(function (err) {
          if (!deferred.isResolved()) {
            deferred.reject(new Error("doc.lock failed - " + self.lockKey));
          }
        });
    }
    return deferred.promise;
  };

  // Wait existing lock release (not create new lock)
  _waitUnlock = () => {
    const deferred = P.defer();
    const self = this;
    this.locker
      .acquire(this.lockKey, function () {
        deferred.resolve();
      })
      .catch(function (err) {
        deferred.reject(new Error("doc._waitUnlock failed - " + self.lockKey));
      });
    return deferred.promise;
  };

  _unlock = () => {
    if (this.connId === null) {
      return;
    }

    this.connId = null;
    const releaseCallback = this.releaseCallback;
    this.releaseCallback = null;

    releaseCallback();

    this.emit("unlock");
  };

  _getChanged = () => {
    return this.changed !== undefined ? this.changed : this.commited;
  };

  _getCommited = () => {
    return this.commited;
  };

  commit = (connId) => {
    this.ensureLocked(connId);

    if (this.changed !== undefined) {
      this.commited = this.changed;
    }
    this.changed = undefined;

    this.emit("commit");
    this._unlock();
  };

  rollback = (connId) => {
    this.ensureLocked(connId);

    this.changed = undefined;

    this.savedIndexValues = {};

    this.emit("rollback");
    this._unlock();
  };

  ensureLocked = (connId) => {
    if (!this.isLocked(connId)) {
      throw new Error("doc not locked by " + connId);
    }
  };

  isLocked = (connId) => {
    return this.connId === connId && connId !== null && connId !== undefined;
  };

  isFree = () => {
    return this.connId === null;
  };

  _getIndexValue = (indexKey, opts) => {
    opts = opts || {};

    const self = this;
    const indexValue = JSON.parse(indexKey)
      .sort()
      .map(function (key) {
        const doc = self._getChanged();
        const value = !!doc ? doc[key] : undefined;
        // null and undefined is not included in index
        if (value === null || value === undefined) {
          return undefined;
        }
        if (["number", "string", "boolean"].indexOf(typeof value) === -1) {
          throw new Error("invalid value for indexed key " + indexKey);
        }
        const ignores = opts.valueIgnore ? opts.valueIgnore[key] || [] : [];
        if (ignores.indexOf(value) !== -1) {
          return undefined;
        }
        return value;
      });

    // Return null if one of the value is undefined
    if (indexValue.indexOf(undefined) !== -1) {
      return null;
    }
    return JSON.stringify(indexValue);
  };
}

export default Document;
