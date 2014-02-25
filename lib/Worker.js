var zmq = require('zmq');
var debug = require('debug')('ZMQ-OMDP');
var util = require('util');
var events = require('events');
var MDP = require('./consts');

var HEARTBEAT_LIVENESS = 3;

function Worker (broker, service, name) {
    var self = this;

    self.name = name || 'worker' + process.pid;

    self.broker = broker;
    self.service = service;

    self.heartbeat = 2500;
    self.reconnect = 2500;

    events.EventEmitter.call(this);
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.start = function () {
    var self = this;
    self.connectToBroker();
};

Worker.prototype.stop = function () {
    var self = this;

    clearInterval(self.hbTimer);
    if (self.socket) {
        self.sendDisconnect();
        self.socket.close();
        delete self['socket'];
    }
};

// Connect or reconnect to broker
Worker.prototype.connectToBroker = function () {
    var self = this;
    
    if (self.socket) {
		clearInterval(self.hbTimer);
        self.socket.close();
    }
	 
	self.socket = zmq.socket('dealer');
    self.socket.identity = new Buffer(self.name + (new Date().getTime()));
    self.socket.setsockopt('linger', 1);

    self.socket.on('message', function () {
        self.onMsg.call(self, arguments);
    });

    self.socket.connect(self.broker);

    debug('Worker connected to %s', self.broker);
    self.sendReady();
    self.liveness = HEARTBEAT_LIVENESS;
	self.rid = undefined;
	self.ridLiveness = HEARTBEAT_LIVENESS;
   
    self.hbTimer = setInterval(function () {
        self.sendHeartbeat();
        self.liveness--;
		if (self.rid) {
			self.ridLiveness--;
		}
        if (self.liveness <= 0 || (self.rid && self.ridLiveness <= 0)) {
            clearInterval(self.hbTimer);
            debug('disconnected from broker - retrying in %s sec(s)...', (self.reconnect / 1000));
            setTimeout(function () {
                self.connectToBroker();
            }, self.reconnect);
        }
    }, self.heartbeat);
};

// process message from broker
Worker.prototype.onMsg = function (msg) {
    var self = this;

    // console.log('W: --- request in worker ---');
    // for (var i = 0; i < msg.length; i++) {
    //     console.log('  ' + i + ': ', msg[i].toString());
    // }

    var header = msg[0].toString();
    var type = msg[1];

    self.liveness = HEARTBEAT_LIVENESS;

    if (header !== MDP.WORKER) {
        self.emitErr('(onMsg) Invalid message header \'' + header + '\'');
        // send error
        return;
    }
    if (type == MDP.W_REQUEST) {
        var client = msg[2].toString();
        var rid = msg[4].toString();
        var data = msg[5].toString();

        debug('got W_REQUEST:', client, rid, data);
        self.onRequest(client, rid, data);
	} else if (type == MDP.W_HEARTBEAT) {
		if (msg[3]) {
			var rid = msg[3].toString();
			if (self.rid == rid) {
				self.ridLiveness = HEARTBEAT_LIVENESS;
			}
		}
    } else if (type == MDP.W_DISCONNECT) {
        debug('got W_DISCONNECT');
        self.connectToBroker();
    } else {
        self.emitErr('(onMsg) Invalid message type \'' + type.toString() + '\'');
        // send error
        return;
    }
};

Worker.prototype.emitReq = function (input, reply) {
    var self = this;

    self.emit.apply(self, ['request', input, reply]);
};

Worker.prototype.emitErr = function (msg) {
    var self = this;

    self.emit.apply(self, ['error', msg]);
};

Worker.prototype.onRequest = function (client, rid, data) {
    var self = this;

    var input = {};
    try {
        input = JSON.parse(data);
    } catch (e) {
        self.emitErr('(onRequest) Parse ERROR: ' + e.toString());
        self.replyError(client, rid, 'Parse ERROR:' + e);
        return;
    }

	self.rid = rid;
	self.ridLiveness = HEARTBEAT_LIVENESS;

    var reply = {
		rid: rid,
		active: function () {
			return self.rid === reply.rid;
		},
        write: function (output) {
            // Partial reply
			self.replyPartial(client, rid, output);
        },
        end: function (output) {
            // Final reply
            self.replyFinal(client, rid, output);
			self.rid = undefined;
        },
        error: function (code, text) {
            self.replyError(client, rid, code, text);
        }
    };
    self.emitReq(input, reply);
};

Worker.prototype.sendReady = function () {
    this.socket.send([MDP.WORKER, MDP.W_READY, this.service]);
};

Worker.prototype.sendDisconnect = function () {
    this.socket.send([MDP.WORKER, MDP.W_DISCONNECT]);
};

Worker.prototype.sendHeartbeat = function () {
    this.socket.send([MDP.WORKER, MDP.W_HEARTBEAT]);
};

Worker.prototype.replyPartial = function (client, rid, data) {
    var rep = {
        status: 200,
        msg: data
    };
    this.socket.send([MDP.WORKER, MDP.W_PARTIAL, client, '', rid, JSON.stringify(rep)]);
};

Worker.prototype.replyFinal = function (client, rid, data) {
    var rep = {
        status: 200,
        msg: data
    };
    this.socket.send([MDP.WORKER, MDP.W_FINAL, client, '', rid, JSON.stringify(rep)]);
};

Worker.prototype.replyError = function (client, rid, code, data) {
    if (! data) {
        data = code;
        code = 500;
    }
    var rep = {
        status: code,
        msg: data
    };
    this.socket.send([MDP.WORKER, MDP.W_FINAL, client, '', rid, JSON.stringify(rep)]);
};

module.exports = Worker;
