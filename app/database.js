const P = require("bluebird");
const Logger = require("memdb-logger");
const utils = require("./utils");
const util = require("util");
const os = require("os");
const EventEmitter = require("events").EventEmitter;
const Connection = require("./connection");
const Shard = require("./shard");
const consts = require("./consts");
const vm = require("vm");
const AsyncLock = require("async-lock");
const _ = require("lodash");

const DEFAULT_SLOWQUERY = 2000;

// Extend promise
utils.extendPromise(P);

export class Database extends EventEmitter {
  constructor(opts) {
    opts = utils.clone(opts) || {};

    this.logger = Logger.getLogger(
      "memdb",
      __filename,
      "shard:" + opts.shardId
    );

    this.connections = utils.forceHashMap();
    this.connectionLock = new AsyncLock({ Promise: P });

    this.dbWrappers = utils.forceHashMap(); //{connId : dbWrapper}

    this.opsCounter = utils.rateCounter();
    this.tpsCounter = utils.rateCounter();

    opts.slowQuery = opts.slowQuery || DEFAULT_SLOWQUERY;

    // Parse index config
    opts.collections = opts.collections || {};

    Object.keys(opts.collections).forEach(function (name) {
      const collection = opts.collections[name];
      const indexes = {};
      (collection.indexes || []).forEach(function (index) {
        if (!Array.isArray(index.keys)) {
          index.keys = [index.keys];
        }
        const indexKey = JSON.stringify(index.keys.sort());
        if (indexes[indexKey]) {
          throw new Error("Duplicate index keys - " + indexKey);
        }

        delete index.keys;
        indexes[indexKey] = index;
      });
      collection.indexes = indexes;
    });

    this.logger.info("parsed opts: %j", opts);

    this.shard = new Shard(opts);

    this.config = opts;

    this.timeCounter = utils.timeCounter();
  }

  start = () => {
    const self = this;
    return this.shard.start().then(function () {
      self.logger.info("database started");
    });
  };

  stop = (force) => {
    const self = this;

    this.opsCounter.stop();
    this.tpsCounter.stop();

    return P.try(function () {
      // Make sure no new request come anymore

      // Wait for all operations finish
      return utils.waitUntil(function () {
        return !self.connectionLock.isBusy();
      });
    })
      .then(function () {
        self.logger.debug("all requests finished");
        return self.shard.stop(force);
      })
      .then(function () {
        self.logger.info("database stoped");
      });
  };

  connect = () => {
    const connId = utils.uuid();
    const opts = {
      _id: connId,
      shard: this.shard,
      config: this.config,
      logger: this.logger,
    };
    const conn = new Connection(opts);
    this.connections[connId] = conn;

    const self = this;
    const dbWrapper = {};
    consts.collMethods.concat(consts.connMethods).forEach(function (method) {
      dbWrapper[method] = () => {
        return self.execute(connId, method, [].slice.call(arguments));
      };
    });
    this.dbWrappers[connId] = dbWrapper;

    this.logger.info("[conn:%s] connection created", connId);
    return {
      connId: connId,
    };
  };

  disconnect = (connId) => {
    const self = this;
    return P.try(function () {
      const conn = self.getConnection(connId);
      return self.execute(connId, "close", [], { ignoreConcurrent: true });
    }).then(function () {
      delete self.connections[connId];
      delete self.dbWrappers[connId];
      self.logger.info("[conn:%s] connection closed", connId);
    });
  };

  // Execute a command
  execute = (connId, method, args, opts) => {
    opts = opts || {};
    const self = this;

    if (method[0] === "$") {
      // Internal method (allow concurrent call)
      const conn = this.getConnection(connId);
      return conn[method].apply(conn, args);
    }

    if (method === "info") {
      return {
        connId: connId,
        ver: consts.version,
        uptime: process.uptime(),
        mem: process.memoryUsage(),
        // rate for last 1, 5, 15 minutes
        ops: [
          this.opsCounter.rate(60),
          this.opsCounter.rate(300),
          this.opsCounter.rate(900),
        ],
        tps: [
          this.tpsCounter.rate(60),
          this.tpsCounter.rate(300),
          this.tpsCounter.rate(900),
        ],
        lps: [
          this.shard.loadCounter.rate(60),
          this.shard.loadCounter.rate(300),
          this.shard.loadCounter.rate(900),
        ],
        ups: [
          this.shard.unloadCounter.rate(60),
          this.shard.unloadCounter.rate(300),
          this.shard.unloadCounter.rate(900),
        ],
        pps: [
          this.shard.persistentCounter.rate(60),
          this.shard.persistentCounter.rate(300),
          this.shard.persistentCounter.rate(900),
        ],
        counter: this.timeCounter.getCounts(),
      };
    } else if (method === "resetCounter") {
      this.opsCounter.reset();
      this.tpsCounter.reset();
      this.shard.loadCounter.reset();
      this.shard.unloadCounter.reset();
      this.shard.persistentCounter.reset();

      this.timeCounter.reset();
      return;
    } else if (method === "eval") {
      const script = args[0] || "";
      const sandbox = args[1] || {};
      sandbox.require = require;
      sandbox.P = P;
      sandbox._ = _;
      sandbox.db = this.dbWrappers[connId];

      const context = vm.createContext(sandbox);

      return vm.runInContext(script, context);
    }

    // Query in the same connection must execute in series
    // This is usually a client bug here
    if (this.connectionLock.isBusy(connId) && !opts.ignoreConcurrent) {
      const err = new Error(
        util.format(
          "[conn:%s] concurrent query on same connection. %s(%j)",
          connId,
          method,
          args
        )
      );
      this.logger.error(err);
      throw err;
    }

    // Ensure series execution in same connection
    return this.connectionLock.acquire(connId, function (cb) {
      self.logger.debug("[conn:%s] start %s(%j)...", connId, method, args);
      if (method === "commit" || method === "rollback") {
        self.tpsCounter.inc();
      } else {
        self.opsCounter.inc();
      }

      const hrtimer = utils.hrtimer(true);
      const conn = null;

      return P.try(function () {
        conn = self.getConnection(connId);

        const func = conn[method];
        if (typeof func !== "function") {
          throw new Error("unsupported command - " + method);
        }
        return func.apply(conn, args);
      })
        .then(
          function (ret) {
            const timespan = hrtimer.stop();
            const level = timespan < self.config.slowQuery ? "info" : "warn"; // warn slow query
            self.logger[level](
              "[conn:%s] %s(%j) => %j (%sms)",
              connId,
              method,
              args,
              ret,
              timespan
            );

            const category = method;
            if (consts.collMethods.indexOf(method) !== -1) {
              category += ":" + args[0];
            }
            self.timeCounter.add(category, timespan);

            return ret;
          },
          function (err) {
            const timespan = hrtimer.stop();
            self.logger.error(
              "[conn:%s] %s(%j) => %s (%sms)",
              connId,
              method,
              args,
              err.stack ? err.stack : err,
              timespan
            );

            if (conn) {
              conn.rollback();
            }

            // Rethrow to client
            throw err;
          }
        )
        .nodeify(cb);
    });
  };

  getConnection = (id) => {
    const conn = this.connections[id];
    if (!conn) {
      throw new Error("connection " + id + " not exist");
    }
    return conn;
  };
}

// const Database =(opts){
//     // clone since we want to modify it
//     opts = utils.clone(opts) || {};

//     this.logger = Logger.getLogger('memdb', __filename, 'shard:' + opts.shardId);

//     this.connections = utils.forceHashMap();
//     this.connectionLock = new AsyncLock({Promise : P});

//     this.dbWrappers = utils.forceHashMap(); //{connId : dbWrapper}

//     this.opsCounter = utils.rateCounter();
//     this.tpsCounter = utils.rateCounter();

//     opts.slowQuery = opts.slowQuery || DEFAULT_SLOWQUERY;

//     // Parse index config
//     opts.collections = opts.collections || {};

//     Object.keys(opts.collections).forEach(function(name){
//         const collection = opts.collections[name];
//         const indexes = {};
//         (collection.indexes || []).forEach(function(index){
//             if(!Array.isArray(index.keys)){
//                 index.keys = [index.keys];
//             }
//             const indexKey = JSON.stringify(index.keys.sort());
//             if(indexes[indexKey]){
//                 throw new Error('Duplicate index keys - ' + indexKey);
//             }

//             delete index.keys;
//             indexes[indexKey] = index;
//         });
//         collection.indexes = indexes;
//     });

//     this.logger.info('parsed opts: %j', opts);

//     this.shard = new Shard(opts);

//     this.config = opts;

//     this.timeCounter = utils.timeCounter();

//     start =(){
//         const self = this;
//         return this.shard.start()
//         .then(function(){
//             self.logger.info('database started');
//         });
//     };

//     stop =(force){
//         const self = this;

//         this.opsCounter.stop();
//         this.tpsCounter.stop();

//         return P.try(function(){
//             // Make sure no new request come anymore

//             // Wait for all operations finish
//             return utils.waitUntil(function(){
//                 return !self.connectionLock.isBusy();
//             });
//         })
//         .then(function(){
//             self.logger.debug('all requests finished');
//             return self.shard.stop(force);
//         })
//         .then(function(){
//             self.logger.info('database stoped');
//         });
//     };

//     connect =(){
//         const connId = utils.uuid();
//         const opts = {
//             _id : connId,
//             shard : this.shard,
//             config : this.config,
//             logger : this.logger
//         };
//         const conn = new Connection(opts);
//         this.connections[connId] = conn;

//         const self = this;
//         const dbWrapper = {};
//         consts.collMethods.concat(consts.connMethods).forEach(function(method){
//             dbWrapper[method] =(){
//                 return self.execute(connId, method, [].slice.call(arguments));
//             };
//         });
//         this.dbWrappers[connId] = dbWrapper;

//         this.logger.info('[conn:%s] connection created', connId);
//         return {
//             connId : connId,
//         };
//     };

//     disconnect =(connId){
//         const self = this;
//         return P.try(function(){
//             const conn = self.getConnection(connId);
//             return self.execute(connId, 'close', [], {ignoreConcurrent : true});
//         })
//         .then(function(){
//             delete self.connections[connId];
//             delete self.dbWrappers[connId];
//             self.logger.info('[conn:%s] connection closed', connId);
//         });
//     };

//     // Execute a command
//     execute =(connId, method, args, opts){
//         opts = opts || {};
//         const self = this;

//         if(method[0] === '$'){ // Internal method (allow concurrent call)
//             const conn = this.getConnection(connId);
//             return conn[method].apply(conn, args);
//         }

//         if(method === 'info'){
//             return {
//                 connId : connId,
//                 ver : consts.version,
//                 uptime : process.uptime(),
//                 mem : process.memoryUsage(),
//                 // rate for last 1, 5, 15 minutes
//                 ops : [this.opsCounter.rate(60), this.opsCounter.rate(300), this.opsCounter.rate(900)],
//                 tps : [this.tpsCounter.rate(60), this.tpsCounter.rate(300), this.tpsCounter.rate(900)],
//                 lps : [this.shard.loadCounter.rate(60), this.shard.loadCounter.rate(300), this.shard.loadCounter.rate(900)],
//                 ups : [this.shard.unloadCounter.rate(60), this.shard.unloadCounter.rate(300), this.shard.unloadCounter.rate(900)],
//                 pps : [this.shard.persistentCounter.rate(60), this.shard.persistentCounter.rate(300), this.shard.persistentCounter.rate(900)],
//                 counter : this.timeCounter.getCounts(),
//             };
//         }
//         else if(method === 'resetCounter'){
//             this.opsCounter.reset();
//             this.tpsCounter.reset();
//             this.shard.loadCounter.reset();
//             this.shard.unloadCounter.reset();
//             this.shard.persistentCounter.reset();

//             this.timeCounter.reset();
//             return;
//         }
//         else if(method === 'eval'){
//             const script = args[0] || '';
//             const sandbox = args[1] || {};
//             sandbox.require = require;
//             sandbox.P = P;
//             sandbox._ = _;
//             sandbox.db = this.dbWrappers[connId];

//             const context = vm.createContext(sandbox);

//             return vm.runInContext(script, context);
//         }

//         // Query in the same connection must execute in series
//         // This is usually a client bug here
//         if(this.connectionLock.isBusy(connId) && !opts.ignoreConcurrent){
//             const err = new Error(util.format('[conn:%s] concurrent query on same connection. %s(%j)', connId, method, args));
//             this.logger.error(err);
//             throw err;
//         }

//         // Ensure series execution in same connection
//         return this.connectionLock.acquire(connId, function(cb){
//             self.logger.debug('[conn:%s] start %s(%j)...', connId, method, args);
//             if(method === 'commit' || method === 'rollback'){
//                 self.tpsCounter.inc();
//             }
//             else{
//                 self.opsCounter.inc();
//             }

//             const hrtimer = utils.hrtimer(true);
//             const conn = null;

//             return P.try(function(){
//                 conn = self.getConnection(connId);

//                 const func = conn[method];
//                 if(typeof(func) !== 'function'){
//                     throw new Error('unsupported command - ' + method);
//                 }
//                 return func.apply(conn, args);
//             })
//             .then(function(ret){
//                 const timespan = hrtimer.stop();
//                 const level = timespan < self.config.slowQuery ? 'info' : 'warn'; // warn slow query
//                 self.logger[level]('[conn:%s] %s(%j) => %j (%sms)', connId, method, args, ret, timespan);

//                 const category = method;
//                 if(consts.collMethods.indexOf(method) !== -1){
//                     category += ':' + args[0];
//                 }
//                 self.timeCounter.add(category, timespan);

//                 return ret;
//             }, function(err){
//                 const timespan = hrtimer.stop();
//                 self.logger.error('[conn:%s] %s(%j) => %s (%sms)', connId, method, args, err.stack ? err.stack : err, timespan);

//                 if(conn){
//                     conn.rollback();
//                 }

//                 // Rethrow to client
//                 throw err;
//             })
//             .nodeify(cb);
//         });
//     };

//     getConnection =(id){
//         const conn = this.connections[id];
//         if(!conn){
//             throw new Error('connection ' + id + ' not exist');
//         }
//         return conn;
//     };
// };

// util.inherits(Database, EventEmitter);

// const proto = Database.prototype;

// start =(){
//     const self = this;
//     return this.shard.start()
//     .then(function(){
//         self.logger.info('database started');
//     });
// };

// stop =(force){
//     const self = this;

//     this.opsCounter.stop();
//     this.tpsCounter.stop();

//     return P.try(function(){
//         // Make sure no new request come anymore

//         // Wait for all operations finish
//         return utils.waitUntil(function(){
//             return !self.connectionLock.isBusy();
//         });
//     })
//     .then(function(){
//         self.logger.debug('all requests finished');
//         return self.shard.stop(force);
//     })
//     .then(function(){
//         self.logger.info('database stoped');
//     });
// };

// connect =(){
//     const connId = utils.uuid();
//     const opts = {
//         _id : connId,
//         shard : this.shard,
//         config : this.config,
//         logger : this.logger
//     };
//     const conn = new Connection(opts);
//     this.connections[connId] = conn;

//     const self = this;
//     const dbWrapper = {};
//     consts.collMethods.concat(consts.connMethods).forEach(function(method){
//         dbWrapper[method] =(){
//             return self.execute(connId, method, [].slice.call(arguments));
//         };
//     });
//     this.dbWrappers[connId] = dbWrapper;

//     this.logger.info('[conn:%s] connection created', connId);
//     return {
//         connId : connId,
//     };
// };

// disconnect =(connId){
//     const self = this;
//     return P.try(function(){
//         const conn = self.getConnection(connId);
//         return self.execute(connId, 'close', [], {ignoreConcurrent : true});
//     })
//     .then(function(){
//         delete self.connections[connId];
//         delete self.dbWrappers[connId];
//         self.logger.info('[conn:%s] connection closed', connId);
//     });
// };

// // Execute a command
// execute =(connId, method, args, opts){
//     opts = opts || {};
//     const self = this;

//     if(method[0] === '$'){ // Internal method (allow concurrent call)
//         const conn = this.getConnection(connId);
//         return conn[method].apply(conn, args);
//     }

//     if(method === 'info'){
//         return {
//             connId : connId,
//             ver : consts.version,
//             uptime : process.uptime(),
//             mem : process.memoryUsage(),
//             // rate for last 1, 5, 15 minutes
//             ops : [this.opsCounter.rate(60), this.opsCounter.rate(300), this.opsCounter.rate(900)],
//             tps : [this.tpsCounter.rate(60), this.tpsCounter.rate(300), this.tpsCounter.rate(900)],
//             lps : [this.shard.loadCounter.rate(60), this.shard.loadCounter.rate(300), this.shard.loadCounter.rate(900)],
//             ups : [this.shard.unloadCounter.rate(60), this.shard.unloadCounter.rate(300), this.shard.unloadCounter.rate(900)],
//             pps : [this.shard.persistentCounter.rate(60), this.shard.persistentCounter.rate(300), this.shard.persistentCounter.rate(900)],
//             counter : this.timeCounter.getCounts(),
//         };
//     }
//     else if(method === 'resetCounter'){
//         this.opsCounter.reset();
//         this.tpsCounter.reset();
//         this.shard.loadCounter.reset();
//         this.shard.unloadCounter.reset();
//         this.shard.persistentCounter.reset();

//         this.timeCounter.reset();
//         return;
//     }
//     else if(method === 'eval'){
//         const script = args[0] || '';
//         const sandbox = args[1] || {};
//         sandbox.require = require;
//         sandbox.P = P;
//         sandbox._ = _;
//         sandbox.db = this.dbWrappers[connId];

//         const context = vm.createContext(sandbox);

//         return vm.runInContext(script, context);
//     }

//     // Query in the same connection must execute in series
//     // This is usually a client bug here
//     if(this.connectionLock.isBusy(connId) && !opts.ignoreConcurrent){
//         const err = new Error(util.format('[conn:%s] concurrent query on same connection. %s(%j)', connId, method, args));
//         this.logger.error(err);
//         throw err;
//     }

//     // Ensure series execution in same connection
//     return this.connectionLock.acquire(connId, function(cb){
//         self.logger.debug('[conn:%s] start %s(%j)...', connId, method, args);
//         if(method === 'commit' || method === 'rollback'){
//             self.tpsCounter.inc();
//         }
//         else{
//             self.opsCounter.inc();
//         }

//         const hrtimer = utils.hrtimer(true);
//         const conn = null;

//         return P.try(function(){
//             conn = self.getConnection(connId);

//             const func = conn[method];
//             if(typeof(func) !== 'function'){
//                 throw new Error('unsupported command - ' + method);
//             }
//             return func.apply(conn, args);
//         })
//         .then(function(ret){
//             const timespan = hrtimer.stop();
//             const level = timespan < self.config.slowQuery ? 'info' : 'warn'; // warn slow query
//             self.logger[level]('[conn:%s] %s(%j) => %j (%sms)', connId, method, args, ret, timespan);

//             const category = method;
//             if(consts.collMethods.indexOf(method) !== -1){
//                 category += ':' + args[0];
//             }
//             self.timeCounter.add(category, timespan);

//             return ret;
//         }, function(err){
//             const timespan = hrtimer.stop();
//             self.logger.error('[conn:%s] %s(%j) => %s (%sms)', connId, method, args, err.stack ? err.stack : err, timespan);

//             if(conn){
//                 conn.rollback();
//             }

//             // Rethrow to client
//             throw err;
//         })
//         .nodeify(cb);
//     });
// };

// getConnection =(id){
//     const conn = this.connections[id];
//     if(!conn){
//         throw new Error('connection ' + id + ' not exist');
//     }
//     return conn;
// };

export default Database;
