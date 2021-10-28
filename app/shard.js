import _ from "lodash";
import P from "bluebird";
import Logger from "memdb-logger";
import util from "util";
import redis from "redis";
import AsyncLock from "async-lock";
import { EventEmitter } from "events";
import backends from "./backends";
import Document from "./document"; //jshint ignore:line
import BackendLocker from "./backendlocker";
import Slave from "./slave";
import utils from "./utils";
import AutoConnection from "../lib/autoconnection";

const STATE = {
  INITED: 0,
  STARTING: 1,
  RUNNING: 2,
  STOPING: 3,
  STOPED: 4,
};

// memory limit 1024MB
const DEFAULT_MEMORY_LIMIT = 1024;

// GC check interval
const DEFAULT_GC_INTERVAL = 1000;

// unload doc count per GC cycle
const DEFAULT_GC_COUNT = 100;

// Idle time before doc is unloaded
// tune this to balance memory usage and performance
// set 0 to never
const DEFAULT_IDLE_TIMEOUT = 1800 * 1000;

// Persistent delay after doc has commited (in ms)
// tune this to balance backend data delay and performance
// set 0 to never
const DEFAULT_PERSISTENT_DELAY = 600 * 1000;

// timeout for locking backend doc
const DEFAULT_BACKEND_LOCK_TIMEOUT = 30 * 1000;
// retry interval for backend lock
const DEFAULT_BACKEND_LOCK_RETRY_INTERVAL = 50;

// delay between unload and load
// Can't load again immediately, prevent 'locking hungry' from other shards
const DEFAULT_RELOAD_DELAY = 20;

// timeout for locking doc
const DEFAULT_LOCK_TIMEOUT = 30 * 1000;

// heartbeat settings, must be multiple of 1000
const DEFAULT_HEARTBEAT_INTERVAL = 2 * 1000;
const DEFAULT_HEARTBEAT_TIMEOUT = 5 * 1000;

export class Shard extends EventEmitter {
  constructor(opts) {
    EventEmitter.call(this);

    opts = opts || {};

    this._id = opts.shardId;
    if (!this._id) {
      throw new Error("shardId is empty");
    }
    this._id = this._id.toString();
    if (this._id.indexOf("$") !== -1) {
      throw new Error('shardId can not contain "$"');
    }

    this.logger = Logger.getLogger("memdb", __filename, "shard:" + this._id);

    this.config = {
      locking: opts.locking || {},
      backend: opts.backend || {},
      slave: opts.slave || {},

      shards: opts.shards || {},

      idleTimeout: opts.hasOwnProperty("idleTimeout")
        ? opts.idleTimeout
        : DEFAULT_IDLE_TIMEOUT,
      persistentDelay: opts.hasOwnProperty("persistentDelay")
        ? opts.persistentDelay
        : DEFAULT_PERSISTENT_DELAY,

      heartbeatInterval: opts.heartbeatInterval || DEFAULT_HEARTBEAT_INTERVAL,
      heartbeatTimeout: opts.heartbeatTimeout || DEFAULT_HEARTBEAT_TIMEOUT,
      backendLockTimeout:
        opts.backendLockTimeout || DEFAULT_BACKEND_LOCK_TIMEOUT,
      backendLockRetryInterval:
        opts.backendLockRetryInterval || DEFAULT_BACKEND_LOCK_RETRY_INTERVAL,
      reloadDelay: opts.reloadDelay || DEFAULT_RELOAD_DELAY,
      lockTimeout: opts.lockTimeout || DEFAULT_LOCK_TIMEOUT,

      memoryLimit: opts.memoryLimit || DEFAULT_MEMORY_LIMIT,
      gcCount: opts.gcCount || DEFAULT_GC_COUNT,
      gcInterval: opts.gcInterval || DEFAULT_GC_INTERVAL,

      disableSlave: opts.disableSlave || false,

      collections: opts.collections || {},
    };

    // global locking
    const lockerConf = this.config.locking;
    lockerConf.shardId = this._id;
    lockerConf.heartbeatTimeout = this.config.heartbeatTimeout;
    lockerConf.heartbeatInterval = this.config.heartbeatInterval;
    this.backendLocker = new BackendLocker(lockerConf);

    // backend storage
    const backendConf = this.config.backend;
    backendConf.shardId = this._id;
    this.backend = backends.create(backendConf);

    // slave redis
    const slaveConf = this.config.slave;
    slaveConf.shardId = this._id;
    this.slave = new Slave(slaveConf);

    // memdb client to communicate with other shards
    this.autoconn = new AutoConnection({
      shards: this.config.shards,
      concurrentInConnection: true,
    });

    // Document storage {key : doc}
    this.docs = utils.forceHashMap();

    // Newly commited docs (for incremental _save)
    this.commitedKeys = utils.forceHashMap(); // {key : version}

    // Idle timeout before unload
    this.idleTimeouts = utils.forceHashMap(); // {key : timeout}

    // Doc persistent timeout
    this.persistentTimeouts = utils.forceHashMap(); // {key : timeout}

    // GC interval
    this.gcInterval = null;

    // Lock async operations for each key
    this.keyLock = new AsyncLock({ Promise: P });

    // Task locker
    this.taskLock = new AsyncLock({ Promise: P });

    // Doc locker
    this.docLock = new AsyncLock({
      timeout: this.config.lockTimeout,
      Promise: P,
    });

    // Current concurrent commiting processes
    this.commitingCount = 0;

    // Current key unloading task
    this.unloadingKeys = utils.forceHashMap();

    this.loadCounter = utils.rateCounter();
    this.unloadCounter = utils.rateCounter();
    this.persistentCounter = utils.rateCounter();

    this.state = STATE.INITED;
  }

  start = () => {
    this._ensureState(STATE.INITED);
    this.state = STATE.STARTING;

    return P.bind(this)
      .then(function () {
        return this.backendLocker.start();
      })
      .then(function () {
        return this.backend.start();
      })
      .then(function () {
        if (!this.config.disableSlave) {
          return this.slave.start();
        }
      })
      .then(function () {
        if (!this.config.disableSlave) {
          return this.restoreFromSlave();
        }
      })
      .then(function () {
        this.gcInterval = setInterval(
          this.gc.bind(this),
          this.config.gcInterval
        );

        this.state = STATE.RUNNING;
        this.emit("start");
        this.logger.info("shard started");
      });
  };

  stop = () => {
    this._ensureState(STATE.RUNNING);

    // This will prevent any further requests
    // All commited data will be saved, while uncommited data will be rolled back
    this.state = STATE.STOPING;

    clearInterval(this.gcInterval);

    return P.bind(this)
      .then(function () {
        // Wait for all running task finish
        return this.taskLock.acquire("", function () {});
      })
      .then(function () {
        this.logger.debug("all running tasks finished");

        // Wait for all commit process finish
        const deferred = P.defer();
        const self = this;
        const check = () => {
          if (self.commitingCount <= 0) {
            deferred.resolve();
          } else {
            setTimeout(check, 200);
          }
        };
        check();
        return deferred.promise;
      })
      .then(function () {
        this.logger.debug("all commit processes finished");
        // WARN: Make sure all connections are closed now

        const self = this;
        return P.mapLimit(Object.keys(this.docs), function (key) {
          return self.keyLock
            .acquire(key, function () {
              return self._unload(key);
            })
            .catch(function (e) {
              self.logger.error(e.stack);
            });
        });
      })
      .then(function () {
        this.logger.debug("all docs unloaded");

        this.loadCounter.stop();
        this.unloadCounter.stop();
        this.persistentCounter.stop();

        if (!this.config.disableSlave) {
          return this.slave.stop();
        }
      })
      .then(function () {
        return this.backend.stop();
      })
      .then(function () {
        return this.backendLocker.stop();
      })
      .then(function () {
        return this.autoconn.close();
      })
      .then(function () {
        this.state = STATE.STOPED;
        this.emit("stop");
        this.logger.info("shard stoped");
      });
  };

  find = (connId, key, fields) => {
    this._ensureState(STATE.RUNNING);
    const self = this;

    if (this.docs[key]) {
      //already loaded
      if (this.docs[key].isFree()) {
        // restart idle timer if doc doesn't locked by anyone
        this._cancelIdleTimeout(key);
        this._startIdleTimeout(key);
      }

      const ret = this.docs[key].find(connId, fields);
      self.logger.debug(
        "[conn:%s] find(%s, %j) => %j",
        connId,
        key,
        fields,
        ret
      );
      return ret;
    }

    return this.keyLock.acquire(key, function () {
      return P.try(function () {
        return self._load(key);
      })
        .then(function () {
          return self.docs[key].find(connId, fields);
        })
        .then(function (ret) {
          self.logger.debug(
            "[conn:%s] find(%s, %j) => %j",
            connId,
            key,
            fields,
            ret
          );
          return ret;
        });
    });
  };

  update = (connId, key, doc, opts) => {
    this._ensureState(STATE.RUNNING);

    // Since lock is called before, so doc is loaded for sure
    const ret = this._doc(key).update(connId, doc, opts);

    this.logger.debug(
      "[conn:%s] update(%s, %j, %j) => %s",
      connId,
      key,
      doc,
      opts,
      ret
    );
    return ret;
  };

  insert = (connId, key, doc) => {
    this._ensureState(STATE.RUNNING);

    const ret = this._doc(key).insert(connId, doc);
    this.logger.debug("[conn:%s] insert(%s, %j) => %s", connId, key, doc, ret);
    return ret;
  };

  remove = (connId, key) => {
    this._ensureState(STATE.RUNNING);

    const ret = this._doc(key).remove(connId);
    this.logger.debug("[conn:%s] remove(%s) => %s", connId, key, ret);
    return ret;
  };

  rollback = (connId, keys) => {
    // Skip state check

    if (!Array.isArray(keys)) {
      keys = [keys];
    }

    const self = this;
    keys.forEach(function (key) {
      self._doc(key).rollback(connId);
    });

    this.logger.debug("[conn:%s] rollback(%j)", connId, keys);
  };

  lock = (connId, key) => {
    this._ensureState(STATE.RUNNING);

    if (this.isLocked(connId, key)) {
      return true;
    }

    this.logger.debug("[conn:%s] shard.lock(%s) start", connId, key);

    const self = this;
    return this.keyLock.acquire(key, function () {
      return P.try(function () {
        return self._load(key);
      }).then(function () {
        return self.docs[key].lock(connId).then(
          function () {
            self.logger.debug("[conn:%s] shard.lock(%s) success", connId, key);
            return true;
          },
          function (e) {
            throw new Error(
              util.format("[conn:%s] shard.lock(%s) failed", connId, key)
            );
          }
        );
      });
    });
  };

  commit = (connId, keys) => {
    this._ensureState(STATE.RUNNING);

    if (!Array.isArray(keys)) {
      keys = [keys];
    }
    if (keys.length === 0) {
      return;
    }

    const self = this;

    keys.forEach(function (key) {
      if (!self.isLocked(connId, key)) {
        throw new Error("[conn:%s] %s not locked", connId, key);
      }
    });

    this.commitingCount++;

    // commit is not concurrency safe for same connection.
    // but database.js guarantee that every request from same connection are in series.
    return P.try(function () {
      if (self.config.disableSlave) {
        return;
      }

      // Sync data to slave
      if (keys.length === 1) {
        const key = keys[0];
        const doc = self._doc(key)._getChanged();
        return self.slave.set(key, doc);
      } else {
        const docs = utils.forceHashMap();
        keys.forEach(function (key) {
          docs[key] = self._doc(key)._getChanged();
        });
        return self.slave.setMulti(docs);
      }
      //TODO: possibly loss consistency
      //      if setMulti return failed but actually sccuess
    })
      .then(function () {
        // Real Commit
        keys.forEach(function (key) {
          self._doc(key).commit(connId);
        });

        self.logger.debug("[conn:%s] commit(%j)", connId, keys);
      })
      .finally(function (ret) {
        self.commitingCount--;
      });
  };

  isLocked = (connId, key) => {
    return this.docs[key] && this.docs[key].isLocked(connId);
  };

  findReadOnly = (connId, key, fields) => {
    this._ensureState(STATE.RUNNING);
    const self = this;

    if (this._isLoaded(key)) {
      return this.find(connId, key, fields);
    }
    return P.try(function () {
      return self.backendLocker.getHolderId(key);
    }).then(function (shardId) {
      if (!shardId || shardId === self._id) {
        return self.find(connId, key, fields);
      }
      return self.autoconn.$findReadOnly(shardId, key, fields);
    });
  };

  // Called by other shards
  $unload = (key) => {
    if (this.state !== STATE.RUNNING) {
      return false;
    }
    if (this.unloadingKeys[key]) {
      return false;
    }

    this.unloadingKeys[key] = true;

    const self = this;
    const deferred = P.defer();

    this.keyLock
      .acquire(key, function () {
        if (!self.docs[key]) {
          // possibly timing issue
          // or a redundant backend lock is held caused by unsuccessful unload
          self.logger.warn("this shard does not hold %s", key);

          return P.try(function () {
            return self.slave.del(key);
          })
            .then(function () {
              return self._unlockBackend(key);
            })
            .then(
              function () {
                deferred.resolve(true);
              },
              function (e) {
                deferred.reject(e);
                throw e;
              }
            );
        }

        return P.try(function () {
          return self._unload(key);
        })
          .then(
            function () {
              deferred.resolve(true);
            },
            function (e) {
              deferred.reject(e);
              throw e;
            }
          )
          .delay(self.config.reloadDelay);
      })
      .catch(function (e) {
        self.logger.error(e.stack);
      })
      .finally(function () {
        delete self.unloadingKeys[key];
      });

    return deferred.promise;
  };

  // internal method, not concurrency safe
  _load = (key) => {
    if (this.docs[key]) {
      // already loaded
      return;
    }

    this.logger.debug("start load %s", key);

    const obj = null;

    const self = this;
    return P.try(function () {
      // get backend lock
      return self._lockBackend(key);
    })
      .then(function () {
        const res = self._resolveKey(key);

        return self.backend.get(res.name, res.id);
      })
      .then(function (ret) {
        obj = ret;
        if (!self.config.disableSlave) {
          // Sync data to slave
          return self.slave.set(key, obj);
        }
      })
      .then(function () {
        self._addDoc(key, obj);

        self.loadCounter.inc();
        self.logger.info("loaded %s", key);
      });
  };

  _addDoc = (key, obj) => {
    const self = this;

    const res = this._resolveKey(key);
    const coll = this.config.collections[res.name];
    const indexes = (coll && coll.indexes) || {};

    const opts = {
      _id: res.id,
      doc: obj,
      indexes: indexes,
      locker: this.docLock,
      lockKey: key,
    };
    const doc = new Document(opts);

    this._startIdleTimeout(key);

    doc.on("lock", function () {
      self._cancelIdleTimeout(key);
    });

    doc.on("unlock", function () {
      self._startIdleTimeout(key);
    });

    doc.on("commit", function () {
      self._setCommited(key);

      // delay sometime and persistent to backend
      if (
        !self.persistentTimeouts.hasOwnProperty(key) &&
        self.config.persistentDelay >= 0
      ) {
        self.persistentTimeouts[key] = setTimeout(function () {
          delete self.persistentTimeouts[key];
          return self.keyLock
            .acquire(key, function () {
              return self._persistent(key);
            })
            .catch(function (err) {
              self.logger.error(err.stack);
            });
        }, self.config.persistentDelay);
      }
    });

    doc.on("updateIndex", function (connId, indexKey, oldValue, newValue) {
      // pass event to collection
      self.emit(
        "updateIndex$" + res.name + "$" + connId,
        res.id,
        indexKey,
        oldValue,
        newValue
      );
    });

    // Loaded at this instant
    self.docs[key] = doc;
  };

  // internal method, not concurrency safe
  _unload = (key) => {
    if (!this.docs[key]) {
      //already unloaded
      return;
    }

    this.logger.debug("start unload %s", key);

    const doc = this.docs[key];

    return P.bind(this)
      .then(function () {
        // Wait all existing lock release
        return doc._waitUnlock();
      })
      .then(function () {
        // Persistent immediately
        return this._persistent(key);
      })
      .then(function () {
        if (!this.config.disableSlave) {
          // sync data to slave
          return this.slave.del(key);
        }
      })
      .then(function () {
        this._cancelIdleTimeout(key);

        if (this.persistentTimeouts.hasOwnProperty(key)) {
          clearTimeout(this.persistentTimeouts[key]);
          delete this.persistentTimeouts[key];
        }

        doc.removeAllListeners("commit");
        doc.removeAllListeners("updateIndex");
        doc.removeAllListeners("lock");
        doc.removeAllListeners("unlock");

        // _unloaded at this instant
        delete this.docs[key];

        // Release backend lock
        return this._unlockBackend(key);
      })
      .then(function () {
        this.unloadCounter.inc();

        this.logger.info("unloaded %s", key);
      });
  };

  // internal method, not concurrency safe
  _lockBackend = (key) => {
    const self = this;
    return P.try(function () {
      return self.backendLocker.tryLock(key);
    }).then(function (success) {
      if (success) {
        return;
      }

      const startTick = Date.now();

      const tryLock = (wait) => {
        return P.try(function () {
          return self.backendLocker.getHolderId(key);
        }).then(function (shardId) {
          if (shardId === self._id) {
            // already locked
            return;
          }

          return P.try(function () {
            if (shardId) {
              // notify holder to unload the doc
              return self.autoconn.$unload(shardId, key);
            } else {
              return true;
            }
          })
            .then(function (success) {
              if (success) {
                return self.backendLocker.tryLock(key);
              } else {
                return false;
              }
            })
            .then(function (success) {
              if (success) {
                self.logger.debug(
                  "locked backend doc - %s (%sms)",
                  key,
                  Date.now() - startTick
                );
                return;
              }

              if (Date.now() - startTick >= self.config.backendLockTimeout) {
                throw new Error("lock backend doc - " + key + " timed out");
              }

              // delay some time and try again
              return P.delay(wait / 2 + _.random(wait)).then(function () {
                return tryLock(wait);
              });
            });
        });
      };

      return tryLock(self.config.backendLockRetryInterval);
    });
  };

  _unlockBackend = (key) => {
    return this.backendLocker.unlock(key);
  };

  // internal method, not concurrency safe
  _persistent = (key) => {
    if (!this.commitedKeys.hasOwnProperty(key)) {
      return; // no change
    }

    const doc = this._doc(key)._getCommited();
    const ver = this.commitedKeys[key]; // get current version

    const self = this;
    const res = this._resolveKey(key);

    return this.backend.set(res.name, res.id, doc).then(function () {
      // no new change, remove the flag
      if (self.commitedKeys[key] === ver) {
        delete self.commitedKeys[key];
      }

      self.persistentCounter.inc();
      self.logger.debug("persistented %s", key);
    });
  };

  //TODO: setTimeout is slow, takes 1/100000 sec
  _startIdleTimeout = (key) => {
    if (!this.config.idleTimeout) {
      return;
    }

    const self = this;
    this.idleTimeouts[key] = setTimeout(function () {
      return self.keyLock
        .acquire(key, function () {
          if (self.docs[key]) {
            self.logger.debug("%s idle timed out, will unload", key);
            return self._unload(key);
          }
        })
        .catch(function (e) {
          self.logger.error(e.stack);
        });
    }, this.config.idleTimeout);
  };

  _cancelIdleTimeout = (key) => {
    clearTimeout(this.idleTimeouts[key]);
    delete this.idleTimeouts[key];
  };

  _setCommited = (key) => {
    if (!this.commitedKeys.hasOwnProperty(key)) {
      this.commitedKeys[key] = 0;
    }
    this.commitedKeys[key]++;
  };

  // Flush changes to backend storage
  flushBackend = (connId) => {
    this._ensureState(STATE.RUNNING);
    const self = this;

    return this.taskLock
      .acquire("", function () {
        return P.mapLimit(Object.keys(self.commitedKeys), function (key) {
          return self.keyLock.acquire(key, function () {
            return self._persistent(key);
          });
        });
      })
      .then(function () {
        self.logger.warn("[conn:%s] flushed Backend", connId);
        return true;
      });
  };

  // Garbage collection
  gc = () => {
    if (this.state !== STATE.RUNNING) {
      return;
    }
    if (this.taskLock.isBusy("")) {
      return;
    }

    const self = this;
    return this.taskLock
      .acquire("", function () {
        const usage = process.memoryUsage();
        const memSize = usage.heapUsed;

        if (memSize < self.config.memoryLimit * 1024 * 1024) {
          // Memory not reach limit, no need to gc
          return;
        }

        self.logger.warn(
          "Start GC. Memory usage is too high, please reduce idleTimeout. %j",
          usage
        );

        const startTick = Date.now();

        // remove some doc
        const keys = [],
          count = 0;
        for (const key in self.docs) {
          keys.push(key);
          count++;
          if (count >= self.config.gcCount) {
            break;
          }
        }

        return P.mapLimit(keys, function (key) {
          return self.keyLock
            .acquire(key, function () {
              return self._unload(key);
            })
            .catch(function (e) {
              self.logger.error(e.stack);
            });
        })
          .then(function () {
            self.logger.warn(
              "Finish GC in %s ms. %s docs have been unloaded.",
              Date.now() - startTick,
              keys.length
            );
          })
          .then(function () {
            process.nextTick(self.gc.bind(self));
          });
      })
      .catch(function (e) {
        self.logger.error(e.stack);
      });
  };

  restoreFromSlave = () => {
    this._ensureState(STATE.STARTING);

    return P.bind(this)
      .then(function () {
        return this.slave.getAllKeys();
      })
      .then(function (keys) {
        if (keys.length === 0) {
          return;
        }

        this.logger.error(
          "Server not stopped properly, will restore data from slave"
        );

        return P.bind(this)
          .then(function () {
            return this.slave.getMulti(keys);
          })
          .then(function (items) {
            const self = this;
            return P.mapLimit(Object.keys(items), function (key) {
              return self.keyLock.acquire(key, function () {
                self._addDoc(key, items[key]);
                // persistent all docs to backend
                self._setCommited(key);
                return self._persistent(key);
              });
            });
          })
          .then(function () {
            this.logger.warn("restored %s keys from slave", keys.length);
          });
      });
  };

  _doc = (key) => {
    if (!this.docs.hasOwnProperty(key)) {
      throw new Error(key + " is not loaded");
    }
    return this.docs[key];
  };

  _isLoaded = (key) => {
    return !!this.docs[key];
  };

  // key - collectionName$docId
  _resolveKey = (key) => {
    const i = key.indexOf("$");
    if (i === -1) {
      throw new Error("invalid key: " + key);
    }
    return { name: key.slice(0, i), id: key.slice(i + 1) };
  };

  _ensureState = (state) => {
    if (this.state !== state) {
      throw new Error(
        util.format(
          "Server state is incorrect, expected %s, actual %s",
          state,
          this.state
        )
      );
    }
  };
}

// const Shard =(opts){
//     EventEmitter.call(this);

//     opts = opts || {};

//     this._id = opts.shardId;
//     if(!this._id){
//         throw new Error('shardId is empty');
//     }
//     this._id = this._id.toString();
//     if(this._id.indexOf('$') !== -1){
//         throw new Error('shardId can not contain "$"');
//     }

//     this.logger = Logger.getLogger('memdb', __filename, 'shard:' + this._id);

//     this.config = {
//         locking : opts.locking || {},
//         backend : opts.backend || {},
//         slave : opts.slave || {},

//         shards : opts.shards || {},

//         idleTimeout : opts.hasOwnProperty('idleTimeout') ? opts.idleTimeout : DEFAULT_IDLE_TIMEOUT,
//         persistentDelay : opts.hasOwnProperty('persistentDelay') ?  opts.persistentDelay : DEFAULT_PERSISTENT_DELAY,

//         heartbeatInterval : opts.heartbeatInterval || DEFAULT_HEARTBEAT_INTERVAL,
//         heartbeatTimeout : opts.heartbeatTimeout || DEFAULT_HEARTBEAT_TIMEOUT,
//         backendLockTimeout : opts.backendLockTimeout || DEFAULT_BACKEND_LOCK_TIMEOUT,
//         backendLockRetryInterval : opts.backendLockRetryInterval || DEFAULT_BACKEND_LOCK_RETRY_INTERVAL,
//         reloadDelay : opts.reloadDelay || DEFAULT_RELOAD_DELAY,
//         lockTimeout : opts.lockTimeout || DEFAULT_LOCK_TIMEOUT,

//         memoryLimit : opts.memoryLimit || DEFAULT_MEMORY_LIMIT,
//         gcCount : opts.gcCount || DEFAULT_GC_COUNT,
//         gcInterval : opts.gcInterval || DEFAULT_GC_INTERVAL,

//         disableSlave : opts.disableSlave || false,

//         collections : opts.collections || {},
//     };

//     // global locking
//     const lockerConf = this.config.locking;
//     lockerConf.shardId = this._id;
//     lockerConf.heartbeatTimeout = this.config.heartbeatTimeout;
//     lockerConf.heartbeatInterval = this.config.heartbeatInterval;
//     this.backendLocker = new BackendLocker(lockerConf);

//     // backend storage
//     const backendConf = this.config.backend;
//     backendConf.shardId = this._id;
//     this.backend = backends.create(backendConf);

//     // slave redis
//     const slaveConf = this.config.slave;
//     slaveConf.shardId = this._id;
//     this.slave = new Slave(slaveConf);

//     // memdb client to communicate with other shards
//     this.autoconn = new AutoConnection({
//         shards : this.config.shards,
//         concurrentInConnection : true,
//     });

//     // Document storage {key : doc}
//     this.docs = utils.forceHashMap();

//     // Newly commited docs (for incremental _save)
//     this.commitedKeys = utils.forceHashMap(); // {key : version}

//     // Idle timeout before unload
//     this.idleTimeouts = utils.forceHashMap(); // {key : timeout}

//     // Doc persistent timeout
//     this.persistentTimeouts = utils.forceHashMap(); // {key : timeout}

//     // GC interval
//     this.gcInterval = null;

//     // Lock async operations for each key
//     this.keyLock = new AsyncLock({Promise : P});

//     // Task locker
//     this.taskLock = new AsyncLock({Promise : P});

//     // Doc locker
//     this.docLock = new AsyncLock({
//         timeout : this.config.lockTimeout,
//         Promise : P,
//     });

//     // Current concurrent commiting processes
//     this.commitingCount = 0;

//     // Current key unloading task
//     this.unloadingKeys = utils.forceHashMap();

//     this.loadCounter = utils.rateCounter();
//     this.unloadCounter = utils.rateCounter();
//     this.persistentCounter = utils.rateCounter();

//     this.state = STATE.INITED;
// };

// util.inherits(Shard, EventEmitter);

// const proto = Shard.prototype;

// start =() =>{
//     this._ensureState(STATE.INITED);
//     this.state = STATE.STARTING;

//     return P.bind(this)
//     .then(function(){
//         return this.backendLocker.start();
//     })
//     .then(function(){
//         return this.backend.start();
//     })
//     .then(function(){
//         if(!this.config.disableSlave){
//             return this.slave.start();
//         }
//     })
//     .then(function(){
//         if(!this.config.disableSlave){
//             return this.restoreFromSlave();
//         }
//     })
//     .then(function(){
//         this.gcInterval = setInterval(this.gc.bind(this), this.config.gcInterval);

//         this.state = STATE.RUNNING;
//         this.emit('start');
//         this.logger.info('shard started');
//     });
// };

// stop =() =>{
//     this._ensureState(STATE.RUNNING);

//     // This will prevent any further requests
//     // All commited data will be saved, while uncommited data will be rolled back
//     this.state = STATE.STOPING;

//     clearInterval(this.gcInterval);

//     return P.bind(this)
//     .then(function(){
//         // Wait for all running task finish
//         return this.taskLock.acquire('', function(){});
//     })
//     .then(function(){
//         this.logger.debug('all running tasks finished');

//         // Wait for all commit process finish
//         const deferred = P.defer();
//         const self = this;
//         const check =() =>{
//             if(self.commitingCount <= 0){
//                 deferred.resolve();
//             }
//             else{
//                 setTimeout(check, 200);
//             }
//         };
//         check();
//         return deferred.promise;
//     })
//     .then(function(){
//         this.logger.debug('all commit processes finished');
//         // WARN: Make sure all connections are closed now

//         const self = this;
//         return P.mapLimit(Object.keys(this.docs), function(key){
//             return self.keyLock.acquire(key, function(){
//                 return self._unload(key);
//             })
//             .catch(function(e){
//                 self.logger.error(e.stack);
//             });
//         });
//     })
//     .then(function(){
//         this.logger.debug('all docs unloaded');

//         this.loadCounter.stop();
//         this.unloadCounter.stop();
//         this.persistentCounter.stop();

//         if(!this.config.disableSlave){
//             return this.slave.stop();
//         }
//     })
//     .then(function(){
//         return this.backend.stop();
//     })
//     .then(function(){
//         return this.backendLocker.stop();
//     })
//     .then(function(){
//         return this.autoconn.close();
//     })
//     .then(function(){
//         this.state = STATE.STOPED;
//         this.emit('stop');
//         this.logger.info('shard stoped');
//     });
// };

// find =(connId, key, fields){
//     this._ensureState(STATE.RUNNING);
//     const self = this;

//     if(this.docs[key]){ //already loaded
//         if(this.docs[key].isFree()){
//             // restart idle timer if doc doesn't locked by anyone
//             this._cancelIdleTimeout(key);
//             this._startIdleTimeout(key);
//         }

//         const ret = this.docs[key].find(connId, fields);
//         self.logger.debug('[conn:%s] find(%s, %j) => %j', connId, key, fields, ret);
//         return ret;
//     }

//     return this.keyLock.acquire(key, function(){
//         return P.try(function(){
//             return self._load(key);
//         })
//         .then(function(){
//             return self.docs[key].find(connId, fields);
//         })
//         .then(function(ret){
//             self.logger.debug('[conn:%s] find(%s, %j) => %j', connId, key, fields, ret);
//             return ret;
//         });
//     });
// };

// update =(connId, key, doc, opts){
//     this._ensureState(STATE.RUNNING);

//     // Since lock is called before, so doc is loaded for sure
//     const ret = this._doc(key).update(connId, doc, opts);

//     this.logger.debug('[conn:%s] update(%s, %j, %j) => %s', connId, key, doc, opts, ret);
//     return ret;
// };

// insert =(connId, key, doc){
//     this._ensureState(STATE.RUNNING);

//     const ret = this._doc(key).insert(connId, doc);
//     this.logger.debug('[conn:%s] insert(%s, %j) => %s', connId, key, doc, ret);
//     return ret;
// };

// remove =(connId, key){
//     this._ensureState(STATE.RUNNING);

//     const ret = this._doc(key).remove(connId);
//     this.logger.debug('[conn:%s] remove(%s) => %s', connId, key, ret);
//     return ret;
// };

// rollback =(connId, keys){
//     // Skip state check

//     if(!Array.isArray(keys)){
//         keys = [keys];
//     }

//     const self = this;
//     keys.forEach(function(key){
//         self._doc(key).rollback(connId);
//     });

//     this.logger.debug('[conn:%s] rollback(%j)', connId, keys);
// };

// lock =(connId, key){
//     this._ensureState(STATE.RUNNING);

//     if(this.isLocked(connId, key)){
//         return true;
//     }

//     this.logger.debug('[conn:%s] shard.lock(%s) start', connId, key);

//     const self = this;
//     return this.keyLock.acquire(key, function(){
//         return P.try(function(){
//             return self._load(key);
//         })
//         .then(function(){
//             return self.docs[key].lock(connId)
//             .then(function(){
//                 self.logger.debug('[conn:%s] shard.lock(%s) success', connId, key);
//                 return true;
//             }, function(e){
//                 throw new Error(util.format('[conn:%s] shard.lock(%s) failed', connId, key));
//             });
//         });
//     });
// };

// commit =(connId, keys){
//     this._ensureState(STATE.RUNNING);

//     if(!Array.isArray(keys)){
//         keys = [keys];
//     }
//     if(keys.length === 0){
//         return;
//     }

//     const self = this;

//     keys.forEach(function(key){
//         if(!self.isLocked(connId, key)){
//             throw new Error('[conn:%s] %s not locked', connId, key);
//         }
//     });

//     this.commitingCount++;

//     // commit is not concurrency safe for same connection.
//     // but database.js guarantee that every request from same connection are in series.
//     return P.try(function(){
//         if(self.config.disableSlave){
//             return;
//         }

//         // Sync data to slave
//         if(keys.length === 1){
//             const key = keys[0];
//             const doc = self._doc(key)._getChanged();
//             return self.slave.set(key, doc);
//         }
//         else{
//             const docs = utils.forceHashMap();
//             keys.forEach(function(key){
//                 docs[key] = self._doc(key)._getChanged();
//             });
//             return self.slave.setMulti(docs);
//         }
//         //TODO: possibly loss consistency
//         //      if setMulti return failed but actually sccuess
//     })
//     .then(function(){
//         // Real Commit
//         keys.forEach(function(key){
//             self._doc(key).commit(connId);
//         });

//         self.logger.debug('[conn:%s] commit(%j)', connId, keys);
//     })
//     .finally(function(ret){
//         self.commitingCount--;
//     });
// };

// isLocked =(connId, key){
//     return this.docs[key] && this.docs[key].isLocked(connId);
// };

// findReadOnly =(connId, key, fields){
//     this._ensureState(STATE.RUNNING);
//     const self = this;

//     if(this._isLoaded(key)){
//         return this.find(connId, key, fields);
//     }
//     return P.try(function(){
//         return self.backendLocker.getHolderId(key);
//     })
//     .then(function(shardId){
//         if(!shardId || shardId === self._id){
//             return self.find(connId, key, fields);
//         }
//         return self.autoconn.$findReadOnly(shardId, key, fields);
//     });
// };

// // Called by other shards
// $unload =(key){
//     if(this.state !== STATE.RUNNING){
//         return false;
//     }
//     if(this.unloadingKeys[key]){
//         return false;
//     }

//     this.unloadingKeys[key] = true;

//     const self = this;
//     const deferred = P.defer();

//     this.keyLock.acquire(key, function(){
//         if(!self.docs[key]){
//             // possibly timing issue
//             // or a redundant backend lock is held caused by unsuccessful unload
//             self.logger.warn('this shard does not hold %s', key);

//             return P.try(function(){
//                 return self.slave.del(key);
//             })
//             .then(function(){
//                 return self._unlockBackend(key);
//             })
//             .then(function(){
//                 deferred.resolve(true);
//             }, function(e){
//                 deferred.reject(e);
//                 throw e;
//             });
//         }

//         return P.try(function(){
//             return self._unload(key);
//         })
//         .then(function(){
//             deferred.resolve(true);
//         }, function(e){
//             deferred.reject(e);
//             throw e;
//         })
//         .delay(self.config.reloadDelay);
//     })
//     .catch(function(e){
//         self.logger.error(e.stack);
//     })
//     .finally(function(){
//         delete self.unloadingKeys[key];
//     });

//     return deferred.promise;
// };

// // internal method, not concurrency safe
// _load =(key){
//     if(this.docs[key]){ // already loaded
//         return;
//     }

//     this.logger.debug('start load %s', key);

//     const obj = null;

//     const self = this;
//     return P.try(function(){
//         // get backend lock
//         return self._lockBackend(key);
//     })
//     .then(function(){
//         const res = self._resolveKey(key);

//         return self.backend.get(res.name, res.id);
//     })
//     .then(function(ret){
//         obj = ret;
//         if(!self.config.disableSlave){
//             // Sync data to slave
//             return self.slave.set(key, obj);
//         }
//     })
//     .then(function(){
//         self._addDoc(key, obj);

//         self.loadCounter.inc();
//         self.logger.info('loaded %s', key);
//     });
// };

// _addDoc =(key, obj){
//     const self = this;

//     const res = this._resolveKey(key);
//     const coll = this.config.collections[res.name];
//     const indexes = (coll && coll.indexes) || {};

//     const opts = {
//         _id : res.id,
//         doc: obj,
//         indexes: indexes,
//         locker : this.docLock,
//         lockKey : key,
//     };
//     const doc = new Document(opts);

//     this._startIdleTimeout(key);

//     doc.on('lock', function(){
//         self._cancelIdleTimeout(key);
//     });

//     doc.on('unlock', function(){
//         self._startIdleTimeout(key);
//     });

//     doc.on('commit', function(){
//         self._setCommited(key);

//         // delay sometime and persistent to backend
//         if(!self.persistentTimeouts.hasOwnProperty(key) && self.config.persistentDelay >= 0){
//             self.persistentTimeouts[key] = setTimeout(function(){
//                 delete self.persistentTimeouts[key];
//                 return self.keyLock.acquire(key, function(){
//                     return self._persistent(key);
//                 })
//                 .catch(function(err){
//                     self.logger.error(err.stack);
//                 });
//             }, self.config.persistentDelay);
//         }
//     });

//     doc.on('updateIndex', function(connId, indexKey, oldValue, newValue){
//         // pass event to collection
//         self.emit('updateIndex$' + res.name + '$' + connId, res.id, indexKey, oldValue, newValue);
//     });

//     // Loaded at this instant
//     self.docs[key] = doc;
// };

// // internal method, not concurrency safe
// _unload =(key){
//     if(!this.docs[key]){ //already unloaded
//         return;
//     }

//     this.logger.debug('start unload %s', key);

//     const doc = this.docs[key];

//     return P.bind(this)
//     .then(function(){
//         // Wait all existing lock release
//         return doc._waitUnlock();
//     })
//     .then(function(){
//         // Persistent immediately
//         return this._persistent(key);
//     })
//     .then(function(){
//         if(!this.config.disableSlave){
//             // sync data to slave
//             return this.slave.del(key);
//         }
//     })
//     .then(function(){
//         this._cancelIdleTimeout(key);

//         if(this.persistentTimeouts.hasOwnProperty(key)){
//             clearTimeout(this.persistentTimeouts[key]);
//             delete this.persistentTimeouts[key];
//         }

//         doc.removeAllListeners('commit');
//         doc.removeAllListeners('updateIndex');
//         doc.removeAllListeners('lock');
//         doc.removeAllListeners('unlock');

//         // _unloaded at this instant
//         delete this.docs[key];

//         // Release backend lock
//         return this._unlockBackend(key);
//     })
//     .then(function(){
//         this.unloadCounter.inc();

//         this.logger.info('unloaded %s', key);
//     });
// };

// // internal method, not concurrency safe
// _lockBackend =(key){
//     const self = this;
//     return P.try(function(){
//         return self.backendLocker.tryLock(key);
//     })
//     .then(function(success){
//         if(success){
//             return;
//         }

//         const startTick = Date.now();

//         const tryLock =(wait){
//             return P.try(function(){
//                 return self.backendLocker.getHolderId(key);
//             })
//             .then(function(shardId){
//                 if(shardId === self._id){
//                     // already locked
//                     return;
//                 }

//                 return P.try(function(){
//                     if(shardId){
//                         // notify holder to unload the doc
//                         return self.autoconn.$unload(shardId, key);
//                     }
//                     else{
//                         return true;
//                     }
//                 })
//                 .then(function(success){
//                     if(success){
//                         return self.backendLocker.tryLock(key);
//                     }
//                     else{
//                         return false;
//                     }
//                 })
//                 .then(function(success){
//                     if(success){
//                         self.logger.debug('locked backend doc - %s (%sms)', key, Date.now() - startTick);
//                         return;
//                     }

//                     if(Date.now() - startTick >= self.config.backendLockTimeout){
//                         throw new Error('lock backend doc - ' + key + ' timed out');
//                     }

//                     // delay some time and try again
//                     return P.delay(wait / 2 + _.random(wait))
//                     .then(function(){
//                         return tryLock(wait);
//                     });
//                 });
//             });
//         };

//         return tryLock(self.config.backendLockRetryInterval);
//     });
// };

// _unlockBackend =(key){
//     return this.backendLocker.unlock(key);
// };

// // internal method, not concurrency safe
// _persistent =(key){
//     if(!this.commitedKeys.hasOwnProperty(key)){
//         return; // no change
//     }

//     const doc = this._doc(key)._getCommited();
//     const ver = this.commitedKeys[key]; // get current version

//     const self = this;
//     const res = this._resolveKey(key);

//     return this.backend.set(res.name, res.id, doc)
//     .then(function(){
//         // no new change, remove the flag
//         if(self.commitedKeys[key] === ver){
//             delete self.commitedKeys[key];
//         }

//         self.persistentCounter.inc();
//         self.logger.debug('persistented %s', key);
//     });
// };

// //TODO: setTimeout is slow, takes 1/100000 sec
// _startIdleTimeout =(key){
//     if(!this.config.idleTimeout){
//         return;
//     }

//     const self = this;
//     this.idleTimeouts[key] = setTimeout(function(){
//         return self.keyLock.acquire(key, function(){
//             if(self.docs[key]){
//                 self.logger.debug('%s idle timed out, will unload', key);
//                 return self._unload(key);
//             }
//         })
//         .catch(function(e){
//             self.logger.error(e.stack);
//         });
//     }, this.config.idleTimeout);
// };

// _cancelIdleTimeout =(key){
//     clearTimeout(this.idleTimeouts[key]);
//     delete this.idleTimeouts[key];
// };

// _setCommited =(key){
//     if(!this.commitedKeys.hasOwnProperty(key)){
//         this.commitedKeys[key] = 0;
//     }
//     this.commitedKeys[key]++;
// };

// // Flush changes to backend storage
// flushBackend =(connId){
//     this._ensureState(STATE.RUNNING);
//     const self = this;

//     return this.taskLock.acquire('', function(){
//         return P.mapLimit(Object.keys(self.commitedKeys), function(key){
//             return self.keyLock.acquire(key, function(){
//                 return self._persistent(key);
//             });
//         });
//     })
//     .then(function(){
//         self.logger.warn('[conn:%s] flushed Backend', connId);
//         return true;
//     });
// };

// // Garbage collection
// gc =() =>{
//     if(this.state !== STATE.RUNNING){
//         return;
//     }
//     if(this.taskLock.isBusy('')){
//         return;
//     }

//     const self = this;
//     return this.taskLock.acquire('', function(){
//         const usage = process.memoryUsage();
//         const memSize = usage.heapUsed;

//         if(memSize < self.config.memoryLimit * 1024 * 1024){
//             // Memory not reach limit, no need to gc
//             return;
//         }

//         self.logger.warn('Start GC. Memory usage is too high, please reduce idleTimeout. %j', usage);

//         const startTick = Date.now();

//         // remove some doc
//         const keys = [], count = 0;
//         for(const key in self.docs){
//             keys.push(key);
//             count++;
//             if(count >= self.config.gcCount){
//                 break;
//             }
//         }

//         return P.mapLimit(keys, function(key){
//             return self.keyLock.acquire(key, function(){
//                 return self._unload(key);
//             })
//             .catch(function(e){
//                 self.logger.error(e.stack);
//             });
//         })
//         .then(function(){
//             self.logger.warn('Finish GC in %s ms. %s docs have been unloaded.', Date.now() - startTick, keys.length);
//         })
//         .then(function(){
//             process.nextTick(self.gc.bind(self));
//         });
//     })
//     .catch(function(e){
//         self.logger.error(e.stack);
//     });
// };

// restoreFromSlave =() =>{
//     this._ensureState(STATE.STARTING);

//     return P.bind(this)
//     .then(function(){
//         return this.slave.getAllKeys();
//     })
//     .then(function(keys){
//         if(keys.length === 0){
//             return;
//         }

//         this.logger.error('Server not stopped properly, will restore data from slave');

//         return P.bind(this)
//         .then(function(){
//             return this.slave.getMulti(keys);
//         })
//         .then(function(items){
//             const self = this;
//             return P.mapLimit(Object.keys(items), function(key){
//                 return self.keyLock.acquire(key, function(){
//                     self._addDoc(key, items[key]);
//                     // persistent all docs to backend
//                     self._setCommited(key);
//                     return self._persistent(key);
//                 });
//             });
//         })
//         .then(function(){
//             this.logger.warn('restored %s keys from slave', keys.length);
//         });
//     });
// };

// _doc =(key){
//     if(!this.docs.hasOwnProperty(key)){
//         throw new Error(key + ' is not loaded');
//     }
//     return this.docs[key];
// };

// _isLoaded =(key){
//     return !!this.docs[key];
// };

// // key - collectionName$docId
// _resolveKey =(key){
//     const i = key.indexOf('$');
//     if(i === -1){
//         throw new Error('invalid key: ' + key);
//     }
//     return {name : key.slice(0, i), id : key.slice(i + 1)};
// };

// _ensureState =(state){
//     if(this.state !== state){
//         throw new Error(util.format('Server state is incorrect, expected %s, actual %s', state, this.state));
//     }
// };

export default Shard;
