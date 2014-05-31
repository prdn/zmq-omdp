var zmq = require('zmq');
var debug = require('debug')('ZMQ-OMDP:Client');
var util = require('util');
var events = require('events');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;

function Client (broker) {
    this.broker = broker;

	this.heartbeat = 5000;

    this.reqs = {};
	this.urid = 1;

    events.EventEmitter.call(this);
};
util.inherits(Client, events.EventEmitter);

Client.prototype.start = function() {
    var self = this;

    this.name = 'Client-' + process.pid + Math.ceil(new Date().getTime() * Math.random() * 1000);

    this.socket = zmq.socket('dealer');
    this.socket.identity = new Buffer(this.name);
    this.socket.setsockopt('linger', 0);

    this.socket.on('message', function() {
        self.onMsg.call(self, arguments);
    });

    this.socket.connect(this.broker);
    debug('Client connected to %s', this.broker);
	
	this.liveness = HEARTBEAT_LIVENESS;

	this.hbTimer = setInterval(function() {
		self.sendHeartbeat();
		self.liveness--;
		if (self.liveness <= 0) {
			Object.keys(self.reqs).every(function(rid) {
				var rme = self.reqs[rid];
				rme.finalCb('TIMEOUT');
				return true;
			});
		}
	}, this.heartbeat);
};

Client.prototype.stop = function() {
    if (this.socket) {
		clearInterval(this.hbTimer);
        this.socket.close();
        delete this.socket;
    }
};

Client.prototype.onMsg = function(msg) {
    var header = msg[0].toString();
    var type = msg[1];
    if (header != MDP.CLIENT) {
        this.emitErr('ERR_MSG_HEADER');
        return;
    }

	this.liveness = HEARTBEAT_LIVENESS;

	if (!msg[2]) {
		return;
	}

	var rid = msg[2].toString();
	var rme = this.reqs[rid];

	if (!rme) {
		return;
	}

	var data = msg[3] || null,
	reply = { status: null, msg: null };

	if (data) {
		data = data.toString();
	}
	try {
		data = JSON.parse(data);
	} catch (e) {}
	
	if (data) {
		reply = data;
	}

	if (reply.status == 200) {
		reply.status = null;
	}

	if (type == MDP.W_REPLY_PARTIAL) {
        if (rme.partialCb) {
            rme.partialCb(reply.msg);
        } else {
			this.emitErr('ERR_CALLBACK_PARTIAL');
        }
    } else if (type == MDP.W_REPLY) {
        rme.finalCb(reply.status, reply.msg);
    } else if (type == MDP.W_HEARTBEAT) {
		// do nothing
    } else {
		this.emitErr('ERR_MSG_TYPE');
    }
};

Client.prototype.emitErr = function(msg) {
    this.emit.apply(this, ['error', msg]);
};

Client.prototype.request = function(service, data, partialCb, finalCb) {
    var self = this;
    var rid = this.getRid();

    this.reqs[rid] = {
		rid: rid,
		partialCb: partialCb,
		finalCb: function() {
			finalCb.apply(null, arguments);
			delete self.reqs[rid];
		}
	};

	debug('send request:', rid);
    this.socket.send([MDP.CLIENT, MDP.W_REQUEST, service, rid, JSON.stringify(data)]);
};

Client.prototype.getRid = function() {
    return this.name + (new Date().getTime()) + (this.urid++);
};

Client.prototype.sendHeartbeat = function() {
	this.socket.send([MDP.CLIENT, MDP.W_HEARTBEAT]);
};

module.exports = Client;
