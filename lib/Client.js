var zmq = require('zmq');
var debug = require('debug')('ZMQ-OMDP');
var util = require('util');
var events = require('events');
var MDP = require('./consts');

function Client (broker, name) {
    this.name = name || 'client' + process.pid;
    this.broker = broker;

	this.heartbeat = 2500;

    this.callbacks = {};
	this.urid = 1;

    events.EventEmitter.call(this);
}
util.inherits(Client, events.EventEmitter);

Client.prototype.start = function () {
    var self = this;

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

    var callbacks = self.callbacks[rid];
    var partialCb = callbacks[0];
    var finalCb = callbacks[1];

    var data = msg[4];
    var reply;
    try {
        reply = JSON.parse(data);
    } catch (e) {
        finalCb(500, 'Unable to parse result: ' + e);
        return;
    }

    var status = reply.status;
    if (status === 200) {
        status = null;
    }
    if (type == MDP.C_PARTIAL) {
        // call partialCb
        if (partialCb) {
            partialCb(reply.msg);
        } else {
            self.emitErr('(onMsg) WARNING: Partial callback required by worker');
        }
    } else if (type == MDP.C_FINAL) {
        // call finalCb
        finalCb(status, reply.msg);
        delete self.callbacks[rid];
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

    self.callbacks[rid] = [partialCb, finalCb];
    debug('send request:', rid, JSON.stringify(data));
    self.socket.send([MDP.CLIENT, MDP.C_REQUEST, service, rid, JSON.stringify(data)]);
};

Client.prototype.getRid = function() {
    return this.name + (new Date().getTime()) + (this.urid++);
};

Client.prototype.sendHeartbeat = function () {
	var self = this;

	Object.keys(self.callbacks).every(function(rid) {
		self.socket.send([MDP.CLIENT, MDP.C_HEARTBEAT, '', rid]);
		return true;
	});
};

module.exports = Client;
