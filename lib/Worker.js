var zmq = require('zmq');
var debug = require('debug')('ZMQ-OMDP:Worker');
var util = require('util');
var events = require('events');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;

function Worker (broker, service) {
    this.broker = broker;
    this.service = service;

    this.heartbeat = 2500;
    this.reconnect = 2500;

    events.EventEmitter.call(this);
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.start = function () {
    this.connectToBroker();
};

Worker.prototype.stop = function () {
    clearInterval(this.hbTimer);
    if (this.socket) {
        this.sendDisconnect();
        this.socket.close();
        delete this['socket'];
    }
};

// Connect or reconnect to broker
Worker.prototype.connectToBroker = function () {
	var self = this;

    if (this.socket) {
		clearInterval(this.hbTimer);
        this.socket.close();
    }
    
	this.name = 'Worker-' + process.pid + Math.ceil(new Date().getTime() * Math.random() * 1000);
	 
	this.socket = zmq.socket('dealer');
    this.socket.identity = new Buffer(this.name);
    this.socket.setsockopt('linger', 0);

    this.socket.on('message', function () {
        self.onMsg.call(self, arguments);
    });

    this.socket.connect(this.broker);

    debug('Worker ' + this.name + ' connected to %s', this.broker);
    this.sendReady();
    this.liveness = HEARTBEAT_LIVENESS;
   
    this.hbTimer = setInterval(function () {
        self.sendHeartbeat();
        self.liveness--;
        if (self.liveness <= 0) {
            clearInterval(self.hbTimer);
            debug('Disconnected from broker - retrying in %s sec(s)...', (self.reconnect / 1000));
            setTimeout(function () {
                self.connectToBroker();
            }, self.reconnect);
        }
    }, this.heartbeat);
};

// process message from broker
Worker.prototype.onMsg = function (msg) {
    var header = msg[0].toString();
    var type = msg[1];

	this.liveness = HEARTBEAT_LIVENESS;

    if (header !== MDP.WORKER) {
        this.emitErr('(onMsg) Invalid message header \'' + header + '\'');
        // send error
        return;
    }
    if (type == MDP.W_REQUEST) {
        var clientId = msg[2].toString();
        var rid = msg[4].toString();
        var data = msg[5].toString();

        debug('got W_REQUEST:', clientId, rid);
        this.onRequest(clientId, rid, data);
	} else if (type == MDP.W_HEARTBEAT) {
		// do nothing
    } else if (type == MDP.W_DISCONNECT) {
        debug('got W_DISCONNECT');
        this.connectToBroker();
    } else {
        this.emitErr('(onMsg) Invalid message type \'' + type.toString() + '\'');
    }
};

Worker.prototype.emitReq = function (input, reply) {
    this.emit.apply(this, ['request', input, reply]);
};

Worker.prototype.emitErr = function (msg) {
    this.emit.apply(this, ['error', msg]);
};

Worker.prototype.onRequest = function (clientId, rid, data) {
    var self = this;

    var input = {};
    try {
        input = JSON.parse(data);
    } catch (e) {
        this.emitErr('(onRequest) Parse ERROR: ' + e.toString());
        this.replyError(clientId, rid, 'Parse ERROR:' + e);
        return;
    }

	var workerId = this.name;

    var reply = {
		active: function () {
			return self.name === workerId;
		},
        write: function (output) {
			self.replyPartial(clientId, rid, output);
        },
        end: function (output) {
            self.replyFinal(clientId, rid, output);
        },
        error: function (code, text) {
            self.replyError(clientId, rid, code, text);
        }
    };
    this.emitReq(input, reply);
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

Worker.prototype.replyPartial = function (clientId, rid, data) {
    var rep = {
        status: 200,
        msg: data
    };
    this.socket.send([MDP.WORKER, MDP.W_REPLY_PARTIAL, clientId, '', rid, JSON.stringify(rep)]);
	return true;
};

Worker.prototype.replyFinal = function (clientId, rid, data) {
    var rep = {
        status: 200,
        msg: data
    };
    this.socket.send([MDP.WORKER, MDP.W_REPLY, clientId, '', rid, JSON.stringify(rep)]);
	return true;
};

Worker.prototype.replyError = function (clientId, rid, code, data) {
    if (! data) {
        data = code;
        code = 500;
    }

    var rep = {
        status: code,
        msg: data
    };
    
	this.socket.send([MDP.WORKER, MDP.W_REPLY, clientId, '', rid, JSON.stringify(rep)]);
	return true;
};

module.exports = Worker;
