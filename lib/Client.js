var zmq = require('zmq');
var debug = require('debug')('ZMQ-OMDP:Client');
var util = require('util');
var events = require('events');
var MDP = require('./consts');

var HEARTBEAT_LIVENESS = 3;

function Client (broker) {
    this.broker = broker;

	this.heartbeat = 2500;

    this.rmap = {};
	this.urid = 1;

    events.EventEmitter.call(this);
}
util.inherits(Client, events.EventEmitter);

Client.prototype.start = function () {
    var self = this;

    self.name = 'Client-' + process.pid + Math.ceil(new Date().getTime() * Math.random() * 1000);

    self.socket = zmq.socket('dealer');
    self.socket.identity = new Buffer(self.name);
    self.socket.setsockopt('linger', 1);

    self.socket.on('message', function () {
        self.onMsg.call(self, arguments);
    });

    self.socket.connect(self.broker);
    debug('Client connected to %s', self.broker);

	self.hbTimer = setInterval(function () {
		self.sendHeartbeat();
		Object.keys(self.rmap).every(function(rid) {
			var rme = self.rmap[rid];
			if (rme.running) {
				rme.liveness--;
				if (rme.liveness <= 0) {
					rme.finalCb('TIMEOUT');
					delete self.rmap[rid];
				}
			}
			return true;
		});
	}, self.heartbeat);
};

Client.prototype.stop = function () {
    var self = this;

    if (self.socket) {
		clearInterval(self.hbTimer);
        self.socket.close();
        delete self['socket'];
    }
};

Client.prototype.onMsg = function (msg) {
    var self = this;

    var header = msg[0].toString();
    var type = msg[1];
    if (header != MDP.CLIENT) {
        self.emitErr('(onMsg) Invalid message header \'' + header + '\'');
        // send error
        return;
    }

    var rid = msg[3].toString();

    var rme = self.rmap[rid];
    var partialCb = rme.partialCb;
    var finalCb = rme.finalCb;

    var data = msg[4];
    var reply;
    var status = null;
	if (data) {
		try {
			reply = JSON.parse(data);
		} catch (e) {
			finalCb(500, 'Unable to parse result: ' + e);
			return;
		}
		status = reply.status;
		if (status === 200) {
			status = null;
		}
	}

	if (type == MDP.W_REPLY_PARTIAL) {
        // call partialCb
        if (partialCb) {
            partialCb(reply.msg);
        } else {
            self.emitErr('(onMsg) WARNING: Partial callback required by worker');
        }
    } else if (type == MDP.W_REPLY) {
        // call finalCb
        finalCb(status, reply.msg);
        delete self.rmap[rid];
    } else if (type == MDP.W_HEARTBEAT) {
		rme.liveness = HEARTBEAT_LIVENESS;
		rme.running = true;
    } else {
        self.emitErr('(onMsg) Invalid message type \'' + type.toString() + '\'');
        // send error
        return;
    }
};

Client.prototype.emitErr = function (msg) {
    var self = this;

    self.emit.apply(self, ['error', msg]);
};

Client.prototype.request = function (service, data, partialCb, finalCb) {
    var self = this;

    if (! finalCb) {
        finalCb = partialCb;
        partialCb = undefined;
    }

    var rid = self.getRid();

    self.rmap[rid] = {
		rid: rid,
		partialCb: partialCb, 
		finalCb: finalCb
	};
    debug('send request:', rid, JSON.stringify(data));
    self.socket.send([MDP.CLIENT, MDP.W_REQUEST, service, rid, JSON.stringify(data)]);
};

Client.prototype.getRid = function() {
    return this.name + (new Date().getTime()) + (this.urid++);
};

Client.prototype.sendHeartbeat = function () {
	var self = this;

	Object.keys(self.rmap).every(function(rid) {
		self.socket.send([MDP.CLIENT, MDP.W_HEARTBEAT, '', rid]);
		return true;
	});
};

module.exports = Client;
