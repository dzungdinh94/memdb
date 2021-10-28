const Database = require('./database');
const memdbLogger = require('memdb-logger');
const net = require('net');
const http = require('http');
const P = require('bluebird');
const uuid = require('node-uuid');
const Protocol = require('./protocol');
const utils = require('./utils');
const consts = require('./consts');

const DEFAULT_PORT = 31017;

exports.start = function(opts){
    const deferred = P.defer();

    const logger = memdbLogger.getLogger('memdb', __filename, 'shard:' + opts.shardId);
    logger.warn('starting %s...', opts.shardId);

    const bind = opts.bind || '0.0.0.0';
    const port = opts.port || DEFAULT_PORT;

    const db = new Database(opts);

    const sockets = utils.forceHashMap();

    const _isShutingDown = false;

    const server = net.createServer(function(socket){

        const clientId = uuid.v4();
        sockets[clientId] = socket;

        const connIds = utils.forceHashMap();
        const remoteAddress = socket.remoteAddress;
        const protocol = new Protocol({socket : socket});

        protocol.on('msg', function(msg){
            logger.debug('[conn:%s] %s => %j', msg.connId, remoteAddress, msg);
            const resp = {seq : msg.seq};

            P.try(function(){
                if(msg.method === 'connect'){
                    const clientVersion = msg.args[0];
                    if(parseFloat(clientVersion) < parseFloat(consts.minClientVersion)){
                        throw new Error('client version not supported, please upgrade');
                    }
                    const connId = db.connect().connId;
                    connIds[connId] = true;
                    return {
                        connId : connId,
                    };
                }
                if(!msg.connId){
                    throw new Error('connId is required');
                }
                if(msg.method === 'disconnect'){
                    return db.disconnect(msg.connId)
                    .then(function(){
                        delete connIds[msg.connId];
                    });
                }
                return db.execute(msg.connId, msg.method, msg.args);
            })
            .then(function(ret){
                resp.err = null;
                resp.data = ret;
            }, function(err){
                resp.err = {
                    message : err.message,
                    stack : err.stack,
                };
                resp.data = null;
            })
            .then(function(){
                protocol.send(resp);
                logger.debug('[conn:%s] %s <= %j', msg.connId, remoteAddress, resp);
            })
            .catch(function(e){
                logger.error(e.stack);
            });
        });

        protocol.on('close', function(){
            P.map(Object.keys(connIds), function(connId){
                return db.disconnect(connId);
            })
            .then(function(){
                connIds = utils.forceHashMap();
                delete sockets[clientId];
                logger.info('client %s disconnected', remoteAddress);
            })
            .catch(function(e){
                logger.error(e.stack);
            });
        });

        protocol.on('error', function(e){
            logger.error(e.stack);
        });

        logger.info('client %s connected', remoteAddress);
    });

    server.on('error', function(err){
        logger.error(err.stack);

        if(!deferred.isResolved()){
            deferred.reject(err);
        }
    });

    P.try(function(){
        return P.promisify(server.listen, server)(port, bind);
    })
    .then(function(){
        return db.start();
    })
    .then(function(){
        logger.warn('server started on %s:%s', bind, port);
        deferred.resolve();
    })
    .catch(function(err){
        logger.error(err.stack);
        deferred.reject(err);
    });

    const shutdown = function(){
        logger.warn('receive shutdown signal');

        if(_isShutingDown){
            return;
        }
        _isShutingDown = true;

        return P.try(function(){
            const deferred = P.defer();

            server.once('close', function(){
                logger.debug('on server close');
                deferred.resolve();
            });

            server.close();

            Object.keys(sockets).forEach(function(id){
                try{
                    sockets[id].end();
                    sockets[id].destroy();
                }
                catch(e){
                    logger.error(e.stack);
                }
            });

            return deferred.promise;
        })
        .then(function(){
            return db.stop();
        })
        .catch(function(e){
            logger.error(e.stack);
        })
        .finally(function(){
            logger.warn('server closed');
            memdbLogger.shutdown(function(){
                process.exit(0);
            });
        });
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);

    process.on('uncaughtException', function(err) {
        logger.error('Uncaught exception: %s', err.stack);
    });

    return deferred.promise;
};
