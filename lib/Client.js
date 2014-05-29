var zmq = require('zmq');
var debug = require('debug')('ZMQ-OMDP:Client');
var util = require('util');
var events = require('events');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;

function Client (broker) {
    this.broker = broker;

	this.heartbeat = 2500;

    this.rmap = {};
	this.urid = 1;

    events.EventEmitter.call(this);
};
util.inherits(Client, events.EventEmitter);

Client.prototype.start = function () {
    var self = this;

    this.name = 'Client-' + process.pid + Math.ceil(new Date().getTime() * Math.random() * 1000);

    this.socket = zmq.socket('dealer');
    this.socket.identity = new Buffer(this.name);
    this.socket.setsockopt('linger', 0);

    this.socket.on('message', function () {
        self.onMsg.call(self, arguments);
    });

    this.socket.connect(this.broker);
    debug('Client connected to %s', this.broker);

	this.hbTimer = setInterval(function () {
		self.sendHeartbeat();
		Object.keys(self.rmap).every(function(rid) {
			var rme = self.rmap[rid];
			rme.liveness--;
			if (rme.liveness <= 0) {
				rme.finalCb('TIMEOUT');
			}
			return true;
		});
	}, this.heartbeat);
};

Client.prototype.stop = function () {
    if (this.socket) {
		clearInterval(this.hbTimer);
        this.socket.close();
        delete this.socket;
    }
};

Client.prototype.onMsg = function (msg) {
    var header = msg[0].toString();
    var type = msg[1];
    if (header != MDP.CLIENT) {
        this.emitErr('(onMsg) Invalid message header \'' + header + '\'');
        return;
    }

    var rid = msg[3].toString();

    var rme = this.rmap[rid];
	if (!rme) {
		return;
	}

    var data = msg[4];
    var reply;
    var status = null;
	if (data) {
		try {
			reply = JSON.parse(data);
		} catch (e) {
			rme.finalCb(500, 'Unable to parse result: ' + e);
			return;
		}
		status = reply.status;
		if (status === 200) {
			status = null;
		}
	}

	if (type == MDP.W_REPLY_PARTIAL) {
        if (rme.partialCb) {
            rme.partialCb(reply.msg);
        } else {
            this.emitErr('(onMsg) WARNING: Partial callback required by worker');
        }
    } else if (type == MDP.W_REPLY) {
        rme.finalCb(status, reply.msg);
    } else if (type == MDP.W_HEARTBEAT) {
		rme.liveness = HEARTBEAT_LIVENESS;
    } else {
        this.emitErr('(onMsg) Invalid message type \'' + type.toString() + '\'');
    }
};

Client.prototype.emitErr = function (msg) {
    this.emit.apply(this, ['error', msg]);
};

Client.prototype.request = function (service, data, partialCb, finalCb) {
    var self = this;
    var rid = this.getRid();

    this.rmap[rid] = {
		rid: rid,
		liveness: HEARTBEAT_LIVENESS,
		partialCb: partialCb, 
		finalCb: function() {
			finalCb.apply(null, arguments);
			delete self.rmap[rid];
		}
	};

	debug('send request:', rid, JSON.stringify(data));
    this.socket.send([MDP.CLIENT, MDP.W_REQUEST, service, rid, JSON.stringify(data)]);
};

Client.prototype.getRid = function() {
    return this.name + (new Date().getTime()) + (this.urid++);
};

Client.prototype.sendHeartbeat = function () {
	var self = this;

	Object.keys(this.rmap).every(function(rid) {
		self.socket.send([MDP.CLIENT, MDP.W_HEARTBEAT, '', rid]);
		return true;
	});
};

module.exports = Client;
