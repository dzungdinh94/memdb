// Copyright 2015 dzungdinh94.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

"use strict";

const P = require("bluebird");
const Logger = require("memdb-logger");
const mongodb = P.promisifyAll(require("mongodb"));

export class MongoBackend {
  constructor(opts) {
    opts = opts || {};

    this.config = {
      url: opts.url || "mongodb://localhost/test",
      options: {
        server: {
          socketOptions: { autoReconnect: true },
          reconnectTries: 10000000,
          reconnectInterval: 5000,
        },
      }, //always retry
    };
    this.conn = null;
    this.connected = false;
    this.logger = Logger.getLogger(
      "memdb",
      __filename,
      "shard:" + opts.shardId
    );
  }

  start = () => {
    const self = this;

    return P.promisify(mongodb.MongoClient.connect)(
      this.config.url,
      this.config.options
    ).then(function (ret) {
      self.conn = ret;
      self.connected = true;

      self.conn.on("close", function () {
        self.connected = false;
        self.logger.error("backend mongodb disconnected");
      });

      self.conn.on("reconnect", function () {
        self.connected = true;
        self.logger.warn("backend mongodb reconnected");
      });

      self.conn.on("error", function (err) {
        self.logger.error(err.stack);
      });

      self.logger.info("backend mongodb connected to %s", self.config.url);
    });
  };

  stop = () => {
    const self = this;

    this.conn.removeAllListeners("close");

    return this.conn.closeAsync().then(function () {
      self.logger.info("backend mongodb closed");
    });
  };

  get = (name, id) => {
    this.ensureConnected();
    this.logger.debug("backend mongodb get(%s, %s)", name, id);

    return this.conn.collection(name).findOneAsync({ _id: id });
  };

  // Return an async iterator with .next(cb) signature
  getAll = (name) => {
    this.ensureConnected();
    this.logger.debug("backend mongodb getAll(%s)", name);

    return this.conn.collection(name).findAsync();
  };

  set = (name, id, doc) => {
    this.ensureConnected();
    this.logger.debug("backend mongodb set(%s, %s)", name, id);

    if (!!doc) {
      doc._id = id;
      return this.conn
        .collection(name)
        .updateAsync({ _id: id }, doc, { upsert: true });
    } else {
      return this.conn.collection(name).removeAsync({ _id: id });
    }
  };

  // items : [{name, id, doc}]
  setMulti = (items) => {
    this.ensureConnected();
    this.logger.debug("backend mongodb setMulti");

    const self = this;
    return P.mapLimit(items, function (item) {
      return self.set(item.name, item.id, item.doc);
    });
  };

  // drop table or database
  drop = (name) => {
    this.ensureConnected();
    this.logger.debug("backend mongodb drop %s", name);

    if (!!name) {
      return this.conn
        .collection(name)
        .dropAsync()
        .catch(function (e) {
          // Ignore ns not found error
          if (e.message.indexOf("ns not found") === -1) {
            throw e;
          }
        });
    } else {
      return this.conn.dropDatabaseAsync();
    }
  };

  getCollectionNames = () => {
    return this.conn.collectionsAsync().then(function (collections) {
      return collections.map(function (collection) {
        return collection.s.name;
      });
    });
  };

  ensureConnected = () => {
    if (!this.connected) {
      throw new Error("backend mongodb not connected");
    }
  };
}

export default MongoBackend;
