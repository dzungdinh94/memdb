import util from "util";
import { EventEmitter } from "events";
import P from "bluebird";
import memlogger from "memdb-logger";
const logger = memlogger.getLogger("memdb", __filename);

const DEFAULT_MAX_MSG_LENGTH = 1024 * 1024;

export class Protocol extends EventEmitter {
  constructor(opts) {
    EventEmitter.call(this);
    opts = opts || {};

    this.socket = opts.socket;
    this.socket.setEncoding("utf8");

    this.maxMsgLength = opts.maxMsgLength || DEFAULT_MAX_MSG_LENGTH;

    this.remainLine = "";

    const self = this;
    this.socket.on("data", function (data) {
      // message is json encoded and splited by '\n'
      const lines = data.split("\n");
      for (const i = 0; i < lines.length - 1; i++) {
        try {
          const msg = "";
          if (i === 0) {
            msg = JSON.parse(self.remainLine + lines[i]);
            self.remainLine = "";
          } else {
            msg = JSON.parse(lines[i]);
          }
          self.emit("msg", msg);
        } catch (err) {
          logger.error(err.stack);
        }
      }
      self.remainLine = lines[lines.length - 1];
    });

    this.socket.on("close", function (hadError) {
      self.emit("close", hadError);
    });

    this.socket.on("connect", function () {
      self.emit("connect");
    });

    this.socket.on("error", function (err) {
      self.emit("error", err);
    });

    this.socket.on("timeout", function () {
      self.emit("timeout");
    });
  }
  send = (msg) => {
    const data = JSON.stringify(msg) + "\n";
    if (data.length > this.maxMsgLength) {
      throw new Error("msg length exceed limit");
    }

    const ret = this.socket.write(data);
    if (!ret) {
      logger.warn("socket.write return false");
    }
  };

  disconnect = () => {
    this.socket.end();
  };
}

// = function(opts){
//     EventEmitter.call(this);

//     opts = opts || {};

//     this.socket = opts.socket;
//     this.socket.setEncoding('utf8');

//     this.maxMsgLength = opts.maxMsgLength || DEFAULT_MAX_MSG_LENGTH;

//     this.remainLine = '';

//     const self = this;
//     this.socket.on('data', function(data){
//         // message is json encoded and splited by '\n'
//         const lines = data.split('\n');
//         for(const i=0; i<lines.length - 1; i++){
//             try{
//                 const msg = '';
//                 if(i === 0){
//                     msg = JSON.parse(self.remainLine + lines[i]);
//                     self.remainLine = '';
//                 }
//                 else{
//                     msg = JSON.parse(lines[i]);
//                 }
//                 self.emit('msg', msg);
//             }
//             catch(err){
//                 logger.error(err.stack);
//             }
//         }
//         self.remainLine = lines[lines.length - 1];
//     });

//     this.socket.on('close', function(hadError){
//         self.emit('close', hadError);
//     });

//     this.socket.on('connect', function(){
//         self.emit('connect');
//     });

//     this.socket.on('error', function(err){
//         self.emit('error', err);
//     });

//     this.socket.on('timeout', function(){
//         self.emit('timeout');
//     });
// };

// util.inherits(Protocol, EventEmitter);

// Protocol.prototype.send = function(msg){
//     const data = JSON.stringify(msg) + '\n';
//     if(data.length > this.maxMsgLength){
//         throw new Error('msg length exceed limit');
//     }

//     const ret = this.socket.write(data);
//     if(!ret){
//         logger.warn('socket.write return false');
//     }
// };

// Protocol.prototype.disconnect = function(){
//     this.socket.end();
// };

export default Protocol;
