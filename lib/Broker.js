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

Broker.prototype.start = function (cb) {
    var self = this;

	this.name = 'Broker-' + process.pid + Math.ceil(new Date().getTime() * Math.random() * 1000);
    this.socket = zmq.socket('router');
    this.socket.identity = new Buffer(this.name);
    this.socket.setsockopt('linger', 1);

    this.socket.on('message', function () {
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
			Object.keys(self.rmap).every(function(rid) {
				var rme = self.rmap[rid];
				var obj = [rme.clientId, MDP.CLIENT, MDP.W_HEARTBEAT, '', rid];
				self.socket.send(obj);
				return true;
			});
		}
    }, HEARTBEAT_INTERVAL);
    
    cb();
};

Broker.prototype.stop = function () {
    clearInterval(this.hbTimer);
    if (this.socket) {
        this.socket.close();
        delete this['socket'];
    }
};

Broker.prototype.onMsg = function (msg) {
    var header = msg[1].toString();

	if (header == MDP.CLIENT) {
        this.onClient(msg);
    } else if (header == MDP.WORKER) {
        this.onWorker(msg);
    } else {
        this.emitErr('(onMsg) Invalid message header \'' + header + '\'');
    }
};

Broker.prototype.emitErr = function (msg) {
    this.emit.apply(this, ['error', msg]);
};

Broker.prototype.onClient = function (msg) {
	var type = msg[2];
	
	if (type == MDP.W_REQUEST) {
		var serviceName = msg[3].toString();
		debug("B: REQUEST from clientId: %s, service: %s", msg[0].toString(), serviceName);

		var service = this.serviceRequire(serviceName);
		this.requestQueue(service, msg);
	} else if (type == MDP.W_HEARTBEAT) {
		var rme = this.rmap[msg[4].toString()];
		if (rme) {
			rme.interest = (new Date()).getTime() + HEARTBEAT_EXPIRY;
		}
	}
};

Broker.prototype.onWorker = function (msg) {
	var workerId = msg[0].toString();
	var type = msg[2];

	var workerReady = (workerId in this.workers);
	var worker = this.workerRequire(workerId);

	var clientId;
	var serviceName;
	var i;
	var obj;
	var wstatus = 1;

	if (this.options.obsessive && worker.rid) {
		if (this.rmap[worker.rid]) {
			this.rmap[worker.rid].expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
		} else {
			wstatus = -2;
		}
	}

	if (wstatus >= 0) {
		if (type == MDP.W_READY) {
			serviceName = msg[3].toString();
			debug('B: register worker: %s, service: %s', workerId, serviceName, workerReady ? 'R' : '');
			if (workerReady) {
				wstatus = -2;
			} else {
				worker.service = this.serviceRequire(serviceName);
				worker.service.workers++;
				wstatus = 0;
			}
		} else {
			if (!workerReady) {
				wstatus = -2;
			}
		}
	}

	if (wstatus < 0) {
		this.workerDelete(workerId, true);
		return;
	}

	if (type == MDP.W_REPLY_PARTIAL) {
		debug("B: REPLY_PARTIAL from worker '%s'", workerId);
		clientId = msg[3].toString();
		serviceName = this.workerService(workerId);
		obj = [clientId, MDP.CLIENT, MDP.W_REPLY_PARTIAL, serviceName];
		for (i = 5; i < msg.length; i++) {
			obj.push(msg[i]);
		}
		this.socket.send(obj);
	} else if (type == MDP.W_REPLY) {
		debug("B: REPLY from worker '%s'", workerId);
		clientId = msg[3].toString();
		serviceName = this.workerService(workerId);
		obj = [clientId, MDP.CLIENT, MDP.W_REPLY, serviceName];
		for (i = 5; i < msg.length; i++) {
			obj.push(msg[i]);
		}
		this.socket.send(obj);
		wstatus = 0;
	} else if (type == MDP.W_HEARTBEAT) {
		// do nothing
	} else if (type == MDP.W_DISCONNECT) {
		wstatus = -1;
	}

	if (wstatus >= 0) {
		worker.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
	}
	
	switch (wstatus) {
		case 0:
			this.workerWaiting(worker, true);
		break;
		case -1:
			this.workerDelete(workerId, false);
		break;
		case -2:
			this.workerDelete(workerId, true);
		break;
	}
};

Broker.prototype.workerService = function (workerId) {
    var worker = this.workers[workerId];
    return worker.service.name;
};

Broker.prototype.requestQueue = function (service, msg) {
	var clientId = msg[0].toString();
	var rid = msg[4].toString();

	var rme = this.rmap[rid] = {
		clientId: clientId,
		rid: rid
	};

	if (this.options.obsessive) {
		rme.interest = (new Date()).getTime() + HEARTBEAT_EXPIRY;
	}

	service.requests.push(msg);

	this.serviceDispatch(service);
};

Broker.prototype.serviceDispatch = function (service) {
    // invalidate workers list according to heartbeats
    this.workerPurge();

    while (service.waiting.length && service.requests.length) {
        // get idle worker
        var workerId = service.waiting.shift();
        var i;
        for (i = 0; i < this.waiting.length; i++) {
            if (workerId !== this.waiting[i]) {
                continue;
            }
            this.waiting.splice(i, 1);
            break;
        }

		var worker = this.workers[workerId];

        // get next message
        var nextMsg = service.requests.shift();
		var rid = nextMsg[4].toString();

		if (!this.rmap[rid]) {
			this.workerWaiting(worker);
			continue;
		}

		var rme = this.rmap[rid];

		if (this.options.obsessive) {
			rme.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
		}
		rme.workerId = workerId;
		worker.rid = rid;
        
        var obj = [workerId, MDP.WORKER, MDP.W_REQUEST, rme.clientId, ''];
		for (i = 4; i < nextMsg.length; i++) {
            obj.push(nextMsg[i]);
        }
        this.socket.send(obj);

		if (this.options.obsessive) {
			obj = [rme.clientId, MDP.CLIENT, MDP.W_HEARTBEAT, '', rid];
			this.socket.send(obj);
		}
		// debug("Dispatched: workerId = %s, rid = %s", workerId, rid);
    }
    debug('serviceDispatch: requests=%s, idle=%s', service.requests.length, service.waiting.length);
};

Broker.prototype.workerPurge = function () {
    var self = this;

	Object.keys(this.workers).every(function(workerId) {
		var worker = self.workers[workerId];
		if ((new Date()).getTime() >= worker.expiry) {
			self.workerDelete(workerId, true);
		}
		return true;
	});
};

Broker.prototype.requestPurge = function () {
	if (!this.options.obsessive) {
		return;
	}

    var self = this;

	Object.keys(this.rmap).every(function(rid) {
		var rme = self.rmap[rid];
		if ((new Date()).getTime() >= rme.interest ||
			rme.workerId && (new Date()).getTime() >= rme.expiry) {
			self.requestDelete(rid);
		}
		return true;
	});
};

Broker.prototype.requestDelete = function (rid) {
	delete this.rmap[rid];
};

Broker.prototype.workerWaiting = function (worker, dispatch) {
    // add worker to waiting list
    this.waiting.push(worker.workerId);
    worker.service.waiting.push(worker.workerId);

    worker.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
	if (worker.rid) {
		this.requestDelete(worker.rid);
	}
	delete worker.rid;
    
    // process queried messages
	if (dispatch) {
		this.serviceDispatch(worker.service);
	}
};

Broker.prototype.workerRequire = function (workerId) {
    if (workerId in this.workers) {
        return this.workers[workerId];
    }

	var worker = {
		workerId: workerId
	};
	this.workers[workerId] = worker;
	return worker;
};

Broker.prototype.workerDelete = function (workerId, disconnect) {
    debug('workerDelete \'%s\'', workerId, disconnect);

    if (!(workerId in this.workers)) {
        return;
    }

    var i;
    var worker = this.workers[workerId];
    if (disconnect) {
        this.socket.send([workerId, MDP.WORKER, MDP.W_DISCONNECT]);
    }

	if (worker.rid) {
		this.requestDelete(worker.rid);
	}
	delete worker.rid;

    // remove worker from service's waiting list
    if (worker.service) {
        for (i = 0; i < worker.service.waiting.length; i++) {
            if (workerId !== worker.service.waiting[i]) {
                continue;
            }
            worker.service.waiting.splice(i, 1);
            break;
        }
        worker.service.workers--;
    }

    // remove worker from broker's waiting list
    for (i = 0; i < this.waiting.length; i++) {
        if (workerId !== this.waiting[i]) {
            continue;
        }
        this.waiting.splice(i, 1);
        break;
    }
    
    // remove worker from broker's workers list
    delete this.workers[workerId];
};

Broker.prototype.serviceRequire = function (name) {
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

module.exports = Broker;
