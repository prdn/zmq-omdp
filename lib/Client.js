var zmq = require('zmq');
var Readable = require('readable-stream').Readable;
var debug = require('debug')('ZMQ-OMDP:Client');
var util = require('util');
var events = require('events');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;

function Client(broker) {
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
	
	this.hbTimer = setInterval(function() {
		self.sendHeartbeat();
		Object.keys(self.reqs).every(function(rid) {
			var req = self.reqs[rid];
			if (req.running) {
				req.liveness--;
			}
			if (req.liveness <= 0 || (req.timeout > -1 && ((new Date()).getTime() > req.ts + req.timeout))) {
				req.finalCb('C_TIMEOUT');
			}
			return true;
		});
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
        req.finalCb(status, data);
    } else if (type == MDP.W_HEARTBEAT) {
		req.liveness = HEARTBEAT_LIVENESS;
		req.running = true;
    } else {
		this.emitErr('ERR_MSG_TYPE');
    }
};

Client.prototype.emitErr = function(msg) {
    this.emit.apply(this, ['error', msg]);
};

Client.prototype.requestStream = function(serviceName, data, opts) {
    var readable = new Readable();

    readable._read = function() {}
    
	this.request(
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

Client.prototype.request = function(serviceName, data, _partialCb, _finalCb, opts) {
    var self = this;
    var rid = this.getRid();

	if (typeof _finalCb != 'function') {
		this.emitErr('ERR_REQ_MISSING_FINALCB');
		return;
	}

	if (typeof opts != 'object') {
		opts = {};
	}	
		
	opts.timeout = opts.timeout || 60000;

	var partialCb = _partialCb ? function(data) {
		_partialCb.call(null, self.decode(data));
	} : undefined;

	var finalCb = function(err, data) {
		delete self.reqs[rid];
		_finalCb.call(null, err, self.decode(data));
	};

	this.reqs[rid] = {
		rid: rid,
		running: false,
		liveness: HEARTBEAT_LIVENESS,
		partialCb: partialCb,
		finalCb: finalCb,
		timeout: opts.timeout,
		ts: new Date().getTime(),
		opts: opts
	};

	debug('C: send request', serviceName, rid);
    this.socket.send([
		MDP.CLIENT, MDP.W_REQUEST, serviceName, rid, 
		this.encode(data), JSON.stringify(opts)
	]);
};

Client.prototype.getRid = function() {
    return this.name + (new Date().getTime()) + (this.urid++);
};

Client.prototype.encode = function(data) {
	return data;
};

Client.prototype.decode = function(data) {
	return data;
};

Client.prototype.sendHeartbeat = function() {
	var self = this;
	this.socket.send([MDP.CLIENT, MDP.W_HEARTBEAT]);
	Object.keys(this.reqs).every(function(rid) {
		self.socket.send([MDP.CLIENT, MDP.W_HEARTBEAT, rid]);
		return true;
	});
};

module.exports = Client;
