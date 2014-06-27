var util = require('util');
var debug = require('debug')('ZMQ-OMDP:Broker');
var events = require('events');
var zmq = require('zmq');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;
var HEARTBEAT_INTERVAL = 5000;
var HEARTBEAT_EXPIRY = HEARTBEAT_LIVENESS * HEARTBEAT_INTERVAL;

function Broker (endpoint, options) {
    this.endpoint = endpoint;
	this.options = options || {};

    this.services = {};
    this.workers = {};
	this.reqs = {};

    events.EventEmitter.call(this);
}
util.inherits(Broker, events.EventEmitter);

Broker.prototype.start = function(cb) {
    var self = this;

	this.name = 'Broker-' + process.pid + Math.ceil(new Date().getTime() * Math.random() * 1000);
    this.socket = zmq.socket('router');
    this.socket.identity = new Buffer(this.name);

    this.socket.on('message', function() {
        self.onMsg.call(self, arguments);
    });

    try {
        this.socket.bindSync(this.endpoint);
    } catch (err) {
        cb(err);
        return;
    }

    debug('Broker started on %s', this.endpoint);
    
	this.hbTimer = setInterval(function() {
		self.requestPurge();
		self.workerPurge();

		Object.keys(self.workers).every(function(workerId) {
			var worker = self.workerRequire(workerId);
			self.socket.send([workerId, MDP.WORKER, MDP.W_HEARTBEAT]);
			return true;
		});
    }, HEARTBEAT_INTERVAL);
    
    cb();
};

Broker.prototype.stop = function() {
    clearInterval(this.hbTimer);
    if (this.socket) {
        this.socket.close();
        delete this['socket'];
    }
};

Broker.prototype.onMsg = function(msg) {
    var header = msg[1].toString();

	if (header == MDP.CLIENT) {
        this.onClient(msg);
    } else if (header == MDP.WORKER) {
        this.onWorker(msg);
    } else {
        debug('(onMsg) Invalid message header \'' + header + '\'');
    }
};

Broker.prototype.emitErr = function(msg) {
    this.emit.apply(this, ['error', msg]);
};

Broker.prototype.onClient = function(msg) {
	var clientId = msg[0].toString();
	var type = msg[2];
	
	if (type == MDP.W_REQUEST) {
		var serviceName = msg[3].toString();
		debug("B: REQUEST from clientId: %s, service: %s", clientId, serviceName);
		var service = this.serviceRequire(serviceName);
		this.requestQueue(serviceName, msg);
	} else if (type == MDP.W_HEARTBEAT) {
		if (msg.length == 4) {
			var rid = msg[3].toString();
			var req = this.reqs[rid];
			if (req && req.workerId) {
				this.socket.send([req.workerId, MDP.WORKER, MDP.W_HEARTBEAT, req.clientId, rid]);
			}
		}
	}
};

Broker.prototype.onWorker = function(msg) {
	var workerId = msg[0].toString();
	var type = msg[2];

	var workerReady = (workerId in this.workers);
	var worker = this.workerRequire(workerId);
	var wstatus = 1;
	
	worker.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
	
	if (type == MDP.W_READY) {
		serviceName = msg[3].toString();
		debug('B: register worker: %s, service: %s', workerId, serviceName, workerReady ? 'R' : '');
		if (workerReady) {
			wstatus = -2;
		} else {
			this.workerSetService(workerId, serviceName);
			wstatus = 0;
		}
	} else {
		if (!workerReady) {
			wstatus = -2;
		}
	}

	if (wstatus < 0) {
		this.workerDelete(workerId, true);
		return;
	}

	if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL) {
		var clientId = msg[3].toString();
		var rid = msg[4].toString();
		if (worker.rid == rid) {
			debug("B: REPLY from worker '%s' (%s)", workerId, worker.service.name);
			var obj = [clientId, MDP.CLIENT, type];
			for (var i = 4; i < msg.length; i++) {
				obj.push(msg[i]);
			}
			this.socket.send(obj);
			if (type == MDP.W_REPLY) {
				wstatus = 0;
			}
		} else {
			wstatus = -2;
		}
	} else {
		if (type == MDP.W_HEARTBEAT) {
			//debug("B: HEARTBEAT from worker '%s'", workerId);
			if (msg.length == 5) {
				var clientId = msg[3].toString();
				var rid = msg[4].toString();
				if (worker.rid == rid) {
					this.socket.send([clientId, MDP.CLIENT, MDP.W_HEARTBEAT, worker.rid]);
				} else {
					wstatus = -2;
				}
			}
		} else if (type == MDP.W_DISCONNECT) {
			wstatus = -1;
		}
	}

	switch (wstatus) {
		case 0:
			this.workerWaiting(workerId, true);
		break;
		case -1:
			this.workerDelete(workerId, false);
		break;
		case -2:
			this.workerDelete(workerId, true);
		break;
	}
};

Broker.prototype.requestDelete = function(rid, notify, err) {
	var req = this.reqs[rid];
	
	if (!req) {
		return;
	}
	
	debug('requestDelete \'%s\'', rid);
	
	if (notify) {
		this.socket.send([req.clientId, MDP.CLIENT, MDP.W_REPLY, rid, JSON.stringify({ status: err || 500 })]);
	}
	
	delete this.reqs[rid];
};

Broker.prototype.requestQueue = function(serviceName, msg) {
	var clientId = msg[0].toString();
	var rid = msg[4].toString();
	var opts = msg[6] ? msg[6].toString() : null;

	try { 
		opts = JSON.parse(opts);
	} catch(e) {
		opts = null;
	}
	if (typeof opts != 'object') {
		opts = {};
	}

	var req = this.reqs[rid] = {
		clientId: clientId,
		rid: rid,
		timeout: opts.timeout || 60000,
		ts: (new Date()).getTime()
	};

	var service = this.serviceRequire(serviceName);
	service.requests.push(msg);

	this.serviceDispatch(serviceName);
};

Broker.prototype.requestPurge = function(serviceName, msg) {
	var self = this;

	Object.keys(this.reqs).every(function(rid) {
		var req = self.reqs[rid];
		
		if (req.workerId) {
			return true;
		}

		if (req.timeout > -1 && ((new Date()).getTime() > req.ts + req.timeout)) {
			self.requestDelete(rid, true, 'TIMEOUT');
		}

		return true;
	});
};	

Broker.prototype.workerRequire = function(workerId) {
    if (workerId in this.workers) {
        return this.workers[workerId];
    }

	var worker = {
		workerId: workerId
	};

	this.workers[workerId] = worker;
	return worker;
};

Broker.prototype.workerSetService = function(workerId, serviceName) {
	var worker = this.workerRequire(workerId);
	worker.service = this.serviceRequire(serviceName);
	worker.service.workers++;
};

Broker.prototype.workerUnsetService = function(workerId) {
	var worker = this.workerRequire(workerId);
	
	if (!worker.service) {
		return;
	}

	for (var i = 0; i < worker.service.waiting.length; i++) {
		if (workerId != worker.service.waiting[i]) {
			continue;
		}
	
		worker.service.waiting.splice(i, 1);
	
		break;
	}
	worker.service.workers--;
};

Broker.prototype.workerDelete = function(workerId, disconnect) {
    var worker = this.workerRequire(workerId);

	if (!worker) {
		return;
	}
    
	debug('workerDelete \'%s\'%s (%s)', workerId, disconnect, worker.service ? worker.service.name : 'UNKNOWN');

	if (worker.rid) {
		this.requestDelete(worker.rid, true, 'W_ERROR');
	}

	if (disconnect) {
        this.socket.send([workerId, MDP.WORKER, MDP.W_DISCONNECT]);
    }

    // remove worker from service's waiting list
	this.workerUnsetService(workerId);
    
    // remove worker from broker's workers list
    delete this.workers[workerId];
};

Broker.prototype.workerGetServiceName = function(workerId) {
    var worker = this.workers[workerId];
    return worker.service.name;
};

Broker.prototype.workerWaiting = function(workerId, dispatch) {
	var worker = this.workerRequire(workerId);
	if (worker.service.waiting.indexOf(workerId) > -1) {
		this.workerDelete(workerId, true);
		return;
	}

	if (worker.rid) {
		this.requestDelete(worker.rid);
		delete worker.rid;
	}
    worker.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
    
    // add worker to waiting list
	worker.service.waiting.push(workerId);

    // process queried messages
	if (dispatch) {
		this.serviceDispatch(worker.service.name);
	}
};

Broker.prototype.workerPurge = function() {
    var self = this;

	Object.keys(this.workers).every(function(workerId) {
		var worker = self.workers[workerId];

		if ((new Date()).getTime() >= worker.expiry) {
			debug('workerPurge removing \'%s\'', workerId);
			self.workerDelete(workerId, true);
		}
		return true;
	});
};

Broker.prototype.serviceRequire = function(name) {
    if (name in this.services) {
        return this.services[name];
    }

    var service = {
        name: name,
        requests: [],
        waiting: [],
        workers: 0
    };
    
    this.services[name] = service;
    return service;
};

Broker.prototype.serviceWorkerUnwait = function(serviceName) {
	var service = this.serviceRequire(serviceName);
	if (service.waiting.length) {
		return service.waiting.shift();
	}
	return null;
};

Broker.prototype.serviceDispatch = function(serviceName) {
    // invalidate workers list according to heartbeats
    this.workerPurge();
	
	var service = this.serviceRequire(serviceName);

    while (service.waiting.length && service.requests.length) {
        // get idle worker
		var workerId = this.serviceWorkerUnwait(serviceName);
		var worker = this.workerRequire(workerId);

		if (worker.rid) {
			this.emitErr("ERR_WORKER_REQ_OVERRIDE");
			return;
		}

        // get next message
        var nextMsg = service.requests.shift();
		var rid = nextMsg[4].toString();

		if (!this.reqs[rid]) {
			this.workerWaiting(workerId);
			continue;
		}

		var req = this.reqs[rid];
		req.workerId = workerId;
		worker.rid = rid;

		var obj = [workerId, MDP.WORKER, MDP.W_REQUEST, req.clientId];

		for (i = 4; i < nextMsg.length; i++) {
			obj.push(nextMsg[i]);
		}

		this.socket.send(obj);	
		this.socket.send([req.clientId, MDP.CLIENT, MDP.W_HEARTBEAT, rid]);
		debug("Dispatched: workerId = %s, rid = %s", workerId, rid);
    }
    debug('serviceDispatch: requests=%s, idle=%s', service.requests.length, service.waiting.length);
};

module.exports = Broker;
