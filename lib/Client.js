var zmq = require('zmq');
var Readable = require('readable-stream').Readable;
var debug = require('debug')('ZMQ-OMDP:Client');
var uuid = require('shortid');
var util = require('util');
var events = require('events');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;

function Client(broker, opts) {
	opts = opts || {};

    this.broker = broker;

	this.conf = {
		heartbeat: 5000
	};

	var self = this;

	Object.keys(opts).every(function(k) {
		self.conf[k] = opts[k];
	});

    this.reqs = {};
	this.urid = 1;

    events.EventEmitter.call(this);
};
util.inherits(Client, events.EventEmitter);

Client.prototype.start = function() {
	var self = this;

	this.name = 'C' + uuid.generate();

	this.socket = zmq.socket('dealer');
	this.socket.identity = new Buffer(this.name);

	this.socket.on('message', function() {
		self.onMsg.call(self, arguments);
	});

	this.socket.connect(this.broker);

	if (this.conf.debug) {
		debug('Client connected to %s', this.broker);
	}

	var checking = false;
	this.hbTimer = setInterval(function() {
		if (checking) {
			return;
		}
		checking = true;
		self.heartbeat();
		setImmediate(function() {
			Object.keys(self.reqs).every(function(rid) {
				var req = self.reqs[rid];
				if (req.timeout > -1 && ((new Date()).getTime() > req.lts + req.timeout)) {
					req.finalCb('C_TIMEOUT');
				}
				return true;
			});
			checking = false;
		});
	}, this.conf.heartbeat);
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

	if (msg.length < 3) {
		return;
	}

	var rid = msg[2].toString();
	var req = this.reqs[rid];
	if (!req) {
		return;
	}

	var status = msg[3] || null;
	var data = msg[4] || null;

	if (data) {
		data = data.toString();
	}

	if (status) {
		status = status.toString();
	}

	if (status == 200) {
		status = null;
	}

	if (type == MDP.W_REPLY_PARTIAL) {
		if (req.partialCb) {
			req.partialCb(data);
		} else {
			this.emitErr('ERR_CALLBACK_PARTIAL');
		}
	} else if (type == MDP.W_REPLY) {
		var fargs = [];
		if (status == null) {
			fargs.push(null);
		}
		fargs.push(data);
		req.finalCb.apply(null, fargs);
	} else {
		this.emitErr('ERR_MSG_TYPE');
	}
};

Client.prototype.emitErr = function(msg) {
	this.emit.apply(this, ['error', msg]);
};

Client.prototype.requestStream = function(serviceName, data, opts) {
	var client = this;
	var readable = new Readable();

	readable._read = function() {}
	client.request(
		serviceName, data, 
		function(data) {
			readable.push(data);
		},
		function(err, data) {
			if (err) {
				readable.emit('error', err);
			} else {
				readable.push(data);
			}
			readable.push(null);
		}, opts
	);

	return readable;
};

Client.prototype.request = function(serviceName, data, partialCb, finalCb, opts) {
	var self = this;
	var rid = this.getRid();

	if (typeof finalCb != 'function') {
		process.nextTick(function () {
			self.emitErr('ERR_REQ_MISSING_FINALCB');
			return;
		});
	}

	if (typeof opts != 'object') {
		opts = {};
	}	

	opts.timeout = opts.timeout || 60000;

	var req = this.reqs[rid] = {
		rid: rid,
		timeout: opts.timeout,
		ts: new Date().getTime(),
		opts: opts,
		heartbeat: function() {
			self.heartbeat(rid);
		}
	};

	req.lts = req.ts;

	req.partialCb = partialCb ? function(data) {
		req.lts = new Date().getTime();
		partialCb.call(null, self.decode(data));
	} : undefined;

	req.finalCb = function(err, data) {
		delete self.reqs[rid];
		finalCb.call(null, err ? self.decode(err) : null, self.decode(data));
	};

	if (this.conf.debug) {
		debug('C: send request', serviceName, rid);
	}

	process.nextTick(function () {
		self.socket.send([
			MDP.CLIENT, MDP.W_REQUEST, serviceName, rid, 
			self.encode(data), JSON.stringify(opts)
		]);
	});
};

Client.prototype.getRid = function() {
	return this.name + (this.urid++);
};

Client.prototype.encode = function(data) {
	return data;
};

Client.prototype.decode = function(data) {
	return data;
};

Client.prototype.heartbeat = function(rid) {
	var msg = [MDP.CLIENT, MDP.W_HEARTBEAT];
	if (rid) {
		msg.push(rid);
	}
	this.socket.send(msg);
};

module.exports = Client;
