

const P = require('bluebird');
const backends = require('./backends');
const BackendLocker = require('./backendlocker');
const Document = require('./document'); //jshint ignore:line
const Collection = require('./collection');
const utils = require('./utils');
const logger = require('memdb-logger').getLogger('memdb', __filename);

const ensureShutDown = function(lockingConf){
    lockingConf.shardId = '$';
    lockingConf.heartbeatInterval = 0;
    const backendLocker = new BackendLocker(lockingConf);

    return P.try(function(){
        return backendLocker.start();
    })
    .then(function(){
        return backendLocker.getActiveShards();
    })
    .then(function(shardIds){
        if(shardIds.length > 0){
            throw new Error('You should shutdown all shards first');
        }
    })
    .finally(function(){
        return backendLocker.stop();
    });
};

// dropIndex('field1 field2')
export const drop = function(conf, collName, keys){
    if(!Array.isArray(keys)){
        keys = keys.split(' ');
    }
    const indexKey = JSON.stringify(keys.sort());
    conf.backend.shardId = '$';
    const backend = backends.create(conf.backend);
    const indexCollName = Collection.prototype._indexCollectionName.call({name : collName}, indexKey);

    return ensureShutDown(conf.locking)
    .then(function(){
        return backend.start();
    })
    .then(function(){
        return backend.drop(indexCollName);
    })
    .finally(function(){
        logger.warn('Droped index %s %s', collName, indexKey);
        return backend.stop();
    });
};

// rebuildIndex('field1 field2', {unique : true})
export const rebuild = function(conf, collName, keys, opts){
    opts = opts || {};
    if(!Array.isArray(keys)){
        keys = keys.split(' ');
    }
    const indexKey = JSON.stringify(keys.sort());
    conf.backend.shardId = '$';
    const backend = backends.create(conf.backend);

    const indexCollName = Collection.prototype._indexCollectionName.call({name : collName}, indexKey);

    logger.warn('Start rebuild index %s %s', collName, indexKey);

    return ensureShutDown(conf.locking)
    .then(function(){
        return backend.start();
    })
    .then(function(){
        return backend.drop(indexCollName);
    })
    .then(function(){
        return backend.getAll(collName);
    })
    .then(function(itor){
        return utils.mongoForEach(itor, function(item){
            const indexValue = Document.prototype._getIndexValue.call({_getChanged : function(){return item;}}, indexKey, opts);
            if(!indexValue){
                return;
            }

            return P.try(function(){
                return backend.get(indexCollName, indexValue);
            })
            .then(function(doc){
                if(!doc){
                    doc = {_id: indexValue, ids : []};
                }
                else if(opts.unique){
                    throw new Error('Duplicate value for unique key ' + indexKey);
                }

                if(doc.ids.indexOf(item._id) === -1){
                    doc.ids.push(item._id);
                }
                return backend.set(indexCollName, indexValue, doc);
            });
        });
    })
    .then(function(){
        logger.warn('Finish rebuild index %s %s', collName, indexKey);
        return backend.stop();
    });
};
