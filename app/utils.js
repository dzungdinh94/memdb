// Copyright 2015 rain1017.
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

'use strict';

import _ from 'lodash';
import util from 'util';
import P from 'bluebird';
import child_process from 'child_process';
import uuid from 'node-uuid';

// Add some usefull promise methods
export const extendPromise = function(P){
    // This is designed for large array
    // The original map with concurrency option does not release memory
    P.mapLimit = function(items, fn, limit){
        if(!limit){
            limit = 1000;
        }
        const groups = [];
        const group = [];
        items.forEach(function(item){
            group.push(item);
            if(group.length >= limit){
                groups.push(group);
                group = [];
            }
        });
        if(group.length > 0){
            groups.push(group);
        }

        const results = [];
        const promise = P.resolve();
        groups.forEach(function(group){
            promise = promise.then(function(){
                return P.map(group, fn)
                .then(function(ret){
                    ret.forEach(function(item){
                        results.push(item);
                    });
                });
            });
        });
        return promise.thenReturn(results);
    };

    P.mapSeries = function(items, fn){
        const results = [];
        return P.each(items, function(item){
            return P.try(function(){
                return fn(item);
            })
            .then(function(ret){
                results.push(ret);
            });
        })
        .thenReturn(results);
    };
};

export const uuid = function(){
    return uuid.v4();
};

export const isEmpty = function(obj){
    for(const key in obj){
        return false;
    }
    return true;
};

export const getObjPath = function(obj, path){
    const current = obj;
    path.split('.').forEach(function(field){
        if(!!current){
            current = current[field];
        }
    });
    return current;
};

export const setObjPath = function(obj, path, value){
    if(typeof(obj) !== 'object'){
        throw new Error('not object');
    }
    const current = obj;
    const fields = path.split('.');
    const finalField = fields.pop();
    fields.forEach(function(field){
        if(!current.hasOwnProperty(field)){
            current[field] = {};
        }
        current = current[field];
        if(typeof(current) !== 'object'){
            throw new Error('field ' + path + ' exists and not a object');
        }
    });
    current[finalField] = value;
};

export const deleteObjPath = function(obj, path){
    if(typeof(obj) !== 'object'){
        throw new Error('not object');
    }
    const current = obj;
    const fields = path.split('.');
    const finalField = fields.pop();
    fields.forEach(function(field){
        if(!!current){
            current = current[field];
        }
    });
    if(current !== undefined){
        delete current[finalField];
    }
};

export const clone = function(obj){
    return JSON.parse(JSON.stringify(obj));
};

export const isDict = function(obj){
    return typeof(obj) === 'object' && obj !== null && !Array.isArray(obj);
};

// escape '$' and '.' in field name
// '$abc.def\\g' => '\\u0024abc\\u002edef\\\\g'
export const escapeField = function(str){
    return str.replace(/\\/g, '\\\\').replace(/\$/g, '\\u0024').replace(/\./g, '\\u002e');
};

export const unescapeField = function(str){
    return str.replace(/\\u002e/g, '.').replace(/\\u0024/g, '$').replace(/\\\\/g, '\\');
};

// Async foreach for mongo's cursor
export const mongoForEach = function(itor, func){
    const deferred = P.defer();

    const next = function(err){
        if(err){
            return deferred.reject(err);
        }
        // async iterator with .next(cb)
        itor.next(function(err, value){
            if(err){
                return deferred.reject(err);
            }
            if(value === null){
                return deferred.resolve();
            }
            P.try(function(){
                return func(value);
            })
            .nodeify(next);
        });
    };
    next();

    return deferred.promise;
};

export const remoteExec = function(ip, cmd, opts){
    ip = ip || '127.0.0.1';
    opts = opts || {};
    const user = opts.user || process.env.USER;
    const successCodes = opts.successCodes || [0];

    const child = null;
    // localhost with current user
    if((ip === '127.0.0.1' || ip.toLowerCase() === 'localhost') && user === process.env.USER){
        child = child_process.spawn('bash', ['-c', cmd]);
    }
    // run remote via ssh
    else{
        child = child_process.spawn('ssh', ['-o StrictHostKeyChecking=no', user + '@' + ip, 'bash -c \'' + cmd + '\'']);
    }

    const deferred = P.defer();
    const stdout = '', stderr = '';
    child.stdout.on('data', function(data){
        stdout += data;
    });
    child.stderr.on('data', function(data){
        stderr += data;
    });
    child.on('exit', function(code, signal){
        if(successCodes.indexOf(code) !== -1){
            deferred.resolve(stdout);
        }
        else{
            deferred.reject(new Error(util.format('remoteExec return code %s on %s@%s - %s\n%s', code, user, ip, cmd, stderr)));
        }
    });
    return deferred.promise;
};

export const waitUntil = function(fn, checkInterval){
    if(!checkInterval){
        checkInterval = 100;
    }

    const deferred = P.defer();
    const check = function(){
        if(fn()){
            deferred.resolve();
        }
        else{
            setTimeout(check, checkInterval);
        }
    };
    check();

    return deferred.promise;
};

export const rateCounter = function(opts){
    opts = opts || {};
    const perserveSeconds = opts.perserveSeconds || 3600;
    const sampleSeconds = opts.sampleSeconds || 5;

    const counts = {};
    const cleanInterval = null;

    const getCurrentSlot = function(){
        return Math.floor(Date.now() / 1000 / sampleSeconds);
    };

    const beginSlot = getCurrentSlot();

    const counter = {
        inc : function(){
            const slotNow = getCurrentSlot();
            if(!counts.hasOwnProperty(slotNow)){
                counts[slotNow] = 0;
            }
            counts[slotNow]++;
        },

        reset : function(){
            counts = {};
            beginSlot = getCurrentSlot();
        },

        clean : function(){
            const slotNow = getCurrentSlot();
            Object.keys(counts).forEach(function(slot){
                if(slot < slotNow - Math.floor(perserveSeconds / sampleSeconds)){
                    delete counts[slot];
                }
            });
        },

        rate : function(lastSeconds){
            const slotNow = getCurrentSlot();
            const total = 0;
            const startSlot = slotNow - Math.floor(lastSeconds / sampleSeconds);
            if(startSlot < beginSlot){
                startSlot = beginSlot;
            }
            for(const slot = startSlot; slot < slotNow; slot++){
                total += counts[slot] || 0;
            }
            return total / ((slotNow - startSlot) * sampleSeconds);
        },

        stop : function(){
            clearInterval(cleanInterval);
        },

        counts : function(){
            return counts;
        }
    };

    cleanInterval = setInterval(function(){
        counter.clean();
    }, sampleSeconds * 1000);

    return counter;
};

export const hrtimer = function(autoStart){
    const total = 0;
    const starttime = null;

    const timer = {
        start : function(){
            if(starttime){
                return;
            }
            starttime = process.hrtime();
        },
        stop : function(){
            if(!starttime){
                return;
            }
            const timedelta = process.hrtime(starttime);
            total += timedelta[0] * 1000 + timedelta[1] / 1000000;
            return total;
        },
        total : function(){
            return total; //in ms
        },
    };

    if(autoStart){
        timer.start();
    }
    return timer;
};

export const timeCounter = function(){
    const counts = {};

    return {
        add : function(name, time){
            if(!counts.hasOwnProperty(name)){
                counts[name] = [0, 0, 0]; // total, count, average
            }
            const count = counts[name];
            count[0] += time;
            count[1]++;
            count[2] = count[0] / count[1];
        },
        reset : function(){
            counts = {};
        },
        getCounts : function(){
            return counts;
        },
    };
};


// trick v8 to not use hidden class
// https://github.com/joyent/node/issues/25661
export const forceHashMap = function(){
    const obj = {k : 1};
    delete obj.k;
    return obj;
};
