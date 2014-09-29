var zmq = require('zmq');
var Writable = require('readable-stream').Writable
var debug = require('debug')('ZMQ-OMDP:Worker');
var uuid = require('shortid');
var util = require('util');
var events = require('events');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;

function Worker(broker, service) {
    this.broker = broker;
    this.service = service;

	this.conf = {
		heartbeat: 5000,
		reconnect: 10000
	};

    events.EventEmitter.call(this);
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.start = function() {
    this.connectToBroker();
};

Worker.prototype.stop = function() {
    clearInterval(this.hbTimer);
    if (this.socket) {
        this.sendDisconnect();
        this.socket.close();
        delete this['socket'];
    }
};

// Connect or reconnect to broker
Worker.prototype.connectToBroker = function() {
	var self = this;

    if (this.socket) {
		clearInterval(this.hbTimer);
        this.socket.close();
    }
	this.wait();
    
    this.name = 'W' + uuid.generate();
	 
	this.socket = zmq.socket('dealer');
    this.socket.identity = new Buffer(this.name);

    this.socket.on('message', function() {
        self.onMsg.call(self, arguments);
    });

    this.socket.connect(this.broker);

    debug('Worker ' + this.name + ' connected to %s', this.broker);

    this.sendReady();
    this.liveness = HEARTBEAT_LIVENESS;
   
    this.hbTimer = setInterval(function() {
        self.liveness--;
        if (self.liveness <= 0) {
            clearInterval(self.hbTimer);
            debug('Disconnected from broker - retrying in %s sec(s)...', (self.conf.reconnect / 1000));
            setTimeout(function() {
                self.connectToBroker();
            }, self.conf.reconnect);
			return;
        }
		self.heartbeat();
		if (self.req) {
			self.req.liveness--;
		}
    }, this.conf.heartbeat);
};

// process message from broker
Worker.prototype.onMsg = function(msg) {
    var header = msg[0].toString();
    var type = msg[1];

    if (header != MDP.WORKER) {
        this.emitErr('ERR_MSG_HEADER');
        // send error
        return;
    }
	
	this.liveness = HEARTBEAT_LIVENESS;

	var clientId;
	var rid;

    if (type == MDP.W_REQUEST) {
		clientId = msg[2].toString();
		rid = msg[3].toString();
        //debug('W: W_REQUEST:', clientId, rid);
		this.onRequest(clientId, rid, msg[4].toString());
	} else if (type == MDP.W_HEARTBEAT) {
		if (msg.length == 4) {
			clientId = msg[2].toString();
			rid = msg[3].toString();
			if (this.req && this.req.rid == rid) {
				this.req.liveness = HEARTBEAT_LIVENESS;
			}
		}
	} else if (type == MDP.W_DISCONNECT) {
        debug('W: W_DISCONNECT');
        this.connectToBroker();
    } else {
        this.emitErr('ERR_MSG_TYPE_INVALID');
    }
};

Worker.prototype.emitReq = function(input, reply) {
    this.emit.apply(this, ['request', input, reply]);
};

Worker.prototype.emitErr = function(msg) {
    this.emit.apply(this, ['error', msg]);
};

Worker.prototype.onRequest = function(clientId, rid, data) {
    var self = this;

	this.req = { clientId: clientId, rid: rid, liveness: HEARTBEAT_LIVENESS };

	var ended = false,
	reply = new Writable();

	var _write = reply.write;
	reply.write = function(chunk, encoding, cb) {
		return _write.call(reply, self.encode(chunk), encoding, cb);
	};

	reply._write = function(chunk, encoding, cb) {
		chunk = String(chunk);
		if (ended) {
			self.replyFinal(clientId, rid, chunk);
			self.wait();
		}
		else {
			self.replyPartial(clientId, rid, chunk);
		}
		cb(null);
	};

	reply.active = function() {
		return self.req && self.req.rid == rid && self.req.liveness > 0;
	};

	reply.closed = function() {
		return !self.req || self.req.rid != rid;
	};

	reply.heartbeat = function() {
		self.heartbeat();
	};

	var _end = reply.end;
	reply.end = function() {
		ended = true;
		var ret = _end.apply(reply, arguments);
		
		if (!reply.closed()) {
			self.replyFinal(clientId, rid, null);
		}

		return ret;
	};

	reply.error = function(status, data) {
		if (!data) {
			data = status;
			status = 500;
		}
		self.replyError(clientId, rid, status, self.encode(data));
		self.wait();
	};

	this.emitReq(this.decode(data), reply);
};

Worker.prototype.encode = function(data) {
	return data;
};

Worker.prototype.decode = function(data) {
	return data;
};

Worker.prototype.wait = function() {
	delete this.req;
};

Worker.prototype.sendReady = function() {
    this.socket.send([MDP.WORKER, MDP.W_READY, this.service]);
};

Worker.prototype.sendDisconnect = function() {
    this.socket.send([MDP.WORKER, MDP.W_DISCONNECT]);
};

Worker.prototype.heartbeat = function() {
    this.socket.send([MDP.WORKER, MDP.W_HEARTBEAT]);
};

Worker.prototype.replyPartial = function(clientId, rid, data) {
	if (!this.req || this.req.rid != rid) {
		this.emitErr('ERR_REQ_MISMATCH');
	}
    this.socket.send([MDP.WORKER, MDP.W_REPLY_PARTIAL, clientId, rid, 200, data]);
	return true;
};

Worker.prototype.replyFinal = function(clientId, rid, data) {
	if (!this.req || this.req.rid != rid) {
		this.emitErr('ERR_REQ_MISMATCH');
	}
    this.socket.send([MDP.WORKER, MDP.W_REPLY, clientId, rid, 200, data]);
	return true;
};

Worker.prototype.replyError = function(clientId, rid, status, data) {
	if (!this.req || this.req.rid != rid) {
		this.emitErr('ERR_REQ_MISMATCH');
	}
	this.socket.send([MDP.WORKER, MDP.W_REPLY, clientId, rid, status, data]);
	return true;
};

module.exports = Worker;
