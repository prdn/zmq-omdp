var util = require('util');
var debug = require('debug')('ZMQ-OMDP:Broker');
var events = require('events');
var zmq = require('zmq');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;
var HEARTBEAT_INTERVAL = 2500;
var HEARTBEAT_EXPIRY = HEARTBEAT_LIVENESS * HEARTBEAT_INTERVAL;

function Broker (endpoint, options) {
    this.endpoint = endpoint;
	this.options = options || {};

    this.services = {};
    this.workers = {};
	this.rmap = {};
    this.waiting = [];

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
        self.workerPurge();
        Object.keys(self.workers).every(function(workerId) {
			var obj = [workerId, MDP.WORKER, MDP.W_HEARTBEAT];
            self.socket.send(obj);
			return true;
        });

		if (self.options.obsessive) {
			self.requestPurge();
		}
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
	var type = msg[2];
	
	if (type == MDP.W_REQUEST) {
		var serviceName = msg[3].toString();
		debug("B: REQUEST from clientId: %s, service: %s", msg[0].toString(), serviceName);

		var service = this.serviceRequire(serviceName);
		this.requestQueue(serviceName, msg);
	} else if (type == MDP.W_HEARTBEAT) {
		// do nothing
	}
};

Broker.prototype.onWorker = function(msg) {
	var workerId = msg[0].toString();
	var type = msg[2];

	var workerReady = (workerId in this.workers);
	var worker = this.workerRequire(workerId);
	var wstatus = 1;
	var rid;

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
		rid = msg[4].toString();
		debug("B: REPLY from worker '%s' (%s)", workerId, worker.service.name);
		var obj = [clientId, MDP.CLIENT, type];
		for (var i = 4; i < msg.length; i++) {
			obj.push(msg[i]);
		}
		this.socket.send(obj);
		if (type == MDP.W_REPLY) {
			wstatus = 0;
			this.requestDelete(rid);
		}
	} else {
		if (type == MDP.W_HEARTBEAT) {
			if (msg.length == 5) {
				rid = msg[4].toString();
			}
		} else if (type == MDP.W_DISCONNECT) {
			wstatus = -1;
		}
	}

	if (wstatus >= 0) {
		if (this.options.obsessive && rid) {
			if (this.rmap[rid]) {
				this.rmap[rid].expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
			}
		}
		worker.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
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

Broker.prototype.requestQueue = function(serviceName, msg) {
	var clientId = msg[0].toString();
	var rid = msg[4].toString();

	var rme = this.rmap[rid] = {
		clientId: clientId,
		rid: rid
	};

	var service = this.serviceRequire(serviceName);
	service.requests.push(msg);

	this.serviceDispatch(serviceName);
};

Broker.prototype.requestPurge = function() {
	if (!this.options.obsessive) {
		return;
	}

    var self = this;

	Object.keys(this.rmap).every(function(rid) {
		var rme = self.rmap[rid];
		if (rme.running && (new Date()).getTime() >= rme.expiry) {
			self.requestDelete(rid, true);
		}
		return true;
	});
};

Broker.prototype.requestDelete = function(rid, notify) {
	var rme = this.rmap[rid];
	if (!rme) {
		return;
	}
	debug('requestDelete \'%s\'', rid);
	if (notify) {
		this.socket.send([rme.clientId, MDP.CLIENT, MDP.W_REPLY, rid, JSON.stringify({ status: 'R_EXPIRED' })]);
	}
	delete this.rmap[rid];
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
		if (workerId !== worker.service.waiting[i]) {
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

    if (disconnect) {
        this.socket.send([workerId, MDP.WORKER, MDP.W_DISCONNECT]);
    }

    // remove worker from service's waiting list
	this.workerUnsetService(workerId);
    
	// remove worker from broker's waiting list
	this.workerUnwait(workerId);
 
    // remove worker from broker's workers list
    delete this.workers[workerId];
};

Broker.prototype.workerGetServiceName = function(workerId) {
    var worker = this.workers[workerId];
    return worker.service.name;
};

Broker.prototype.workerWaiting = function(workerId, dispatch) {
    // add worker to waiting list
	var worker = this.workerRequire(workerId);
    worker.service.waiting.push(worker.workerId);
	this.waiting.push(workerId);

    worker.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
    
    // process queried messages
	if (dispatch) {
		this.serviceDispatch(worker.service.name);
	}
};

Broker.prototype.workerUnwait = function(workerId) {
	for (var i = 0; i < this.waiting.length; i++) {
		if (workerId !== this.waiting[i]) {
			continue;
		}
		this.waiting.splice(i, 1);
		break;
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
	var workerId = service.waiting.shift();

	this.workerUnwait(workerId);
	return workerId;
};

Broker.prototype.serviceDispatch = function(serviceName) {
    // invalidate workers list according to heartbeats
    this.workerPurge();
	
	var service = this.serviceRequire(serviceName);

    while (service.waiting.length && service.requests.length) {
        // get idle worker
		var workerId = this.serviceWorkerUnwait(serviceName);
		var worker = this.workerRequire(workerId);

        // get next message
        var nextMsg = service.requests.shift();
		var rid = nextMsg[4].toString();

		if (!this.rmap[rid]) {
			this.workerWaiting(workerId);
			continue;
		}

		var rme = this.rmap[rid];
		rme.running = true;

		if (this.options.obsessive) {
			rme.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
		}
        
        var obj = [workerId, MDP.WORKER, MDP.W_REQUEST, rme.clientId];
		for (i = 4; i < nextMsg.length; i++) {
            obj.push(nextMsg[i]);
        }
        this.socket.send(obj);
		// debug("Dispatched: workerId = %s, rid = %s", workerId, rid);
    }
    debug('serviceDispatch: requests=%s, idle=%s', service.requests.length, service.waiting.length);
};

module.exports = Broker;
