var util = require('util');
var debug = require('debug')('ZMQ-OMDP:Broker');
var events = require('events');
var zmq = require('zmq');
var MDP = require('./consts');

var HEARTBEAT_LIVENESS = 3;
var HEARTBEAT_INTERVAL = 2500;
var HEARTBEAT_EXPIRY = HEARTBEAT_LIVENESS * HEARTBEAT_INTERVAL;

function Broker (endpoint) {
    var self = this;

    self.endpoint = endpoint;

    self.services = {};
    self.workers = {};
	self.rmap = {};
    self.waiting = [];

    events.EventEmitter.call(this);
}
util.inherits(Broker, events.EventEmitter);

Broker.prototype.start = function (cb) {
    var self = this;

	self.name = 'Broker-' + process.pid + Math.ceil(new Date().getTime() * Math.random() * 1000);
    self.socket = zmq.socket('router');
    self.socket.identity = new Buffer(self.name);
    self.socket.setsockopt('linger', 1);

    self.socket.on('message', function () {
        self.onMsg.call(self, arguments);
    });

    try {
        self.socket.bindSync(self.endpoint);
    } catch (err) {
        cb(err);
        return;
    }
    debug('Broker started on %s', self.endpoint);
    
    self.hbTimer = setInterval(function() {
        self.workerPurge();
        Object.keys(self.workers).every(function(identify) {
			var obj = [identify, MDP.WORKER, MDP.W_HEARTBEAT];
			var worker = self.workers[identify];
			if (worker.rid) {
				obj.push('', worker.rid);
			}
            self.socket.send(obj);
			return true;
        });

		self.requestPurge();
        Object.keys(self.rmap).every(function(rid) {
			var rme = self.rmap[rid];
			var obj = [rme.client, MDP.CLIENT, MDP.W_HEARTBEAT, '', rid];
            self.socket.send(obj);
			return true;
        });
    }, HEARTBEAT_INTERVAL);
    
    cb();
};

Broker.prototype.stop = function () {
    var self = this;

    clearInterval(self.hbTimer);
    if (self.socket) {
        self.socket.close();
        delete self['socket'];
    }
};

Broker.prototype.onMsg = function (msg) {
    var self = this;

    var client = msg[0];
    var header = msg[1].toString();

	if (header == MDP.CLIENT) {
        self.onClient(msg);
    } else if (header == MDP.WORKER) {
        self.onWorker(msg);
    } else {
        // send error to client
        self.emitErr('(onMsg) Invalid message header \'' + header + '\'');
    }
};

Broker.prototype.emitErr = function (msg) {
    this.emit.apply(this, ['error', msg]);
};

Broker.prototype.onClient = function (msg) {
    var self = this;
    
	var type = msg[2];
	
	if (type == MDP.W_REQUEST) {
		var serviceName = msg[3].toString();
		debug("B: REQUEST from client: %s, service: %s", msg[0].toString(), serviceName);

		var service = self.serviceRequire(serviceName);
		self.requestQueue(service, msg);
	} else if (type == MDP.W_HEARTBEAT) {
		var rme = self.rmap[msg[4].toString()];
		if (rme) {
			rme.interest = (new Date()).getTime() + HEARTBEAT_EXPIRY;
		}
	}
};

Broker.prototype.onWorker = function (msg) {
	var self = this;

	var identify = msg[0].toString();
	var type = msg[2];

	var workerReady = (identify in self.workers);
	var worker = self.workerRequire(identify);

	var client;
	var serviceName;
	var i;
	var obj;
	var wstatus = 1;

	if (worker.rid) {
		if (self.rmap[worker.rid]) {
			self.rmap[worker.rid].expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
		} else {
			wstatus = -2;
		}
	}

	if (wstatus >= 0) {
		if (type == MDP.W_READY) {
			serviceName = msg[3].toString();
			debug('B: register worker: %s, service: %s', identify, serviceName, workerReady ? 'R' : '');
			if (workerReady) {
				wstatus = -2;
			} else {
				worker.service = self.serviceRequire(serviceName);
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
		self.workerDelete(identify, true);
		return;
	}

	if (type == MDP.W_REPLY_PARTIAL) {
		// debug("B: REPLY_PARTIAL from worker '%s'", identify);
		client = msg[3];
		serviceName = self.workerService(identify);
		obj = [client, MDP.CLIENT, MDP.W_REPLY_PARTIAL, serviceName];
		for (i = 5; i < msg.length; i++) {
			obj.push(msg[i]);
		}
		self.socket.send(obj);
	} else if (type == MDP.W_REPLY) {
		debug("B: REPLY from worker '%s'", identify);
		client = msg[3];
		serviceName = self.workerService(identify);
		obj = [client, MDP.CLIENT, MDP.W_REPLY, serviceName];
		for (i = 5; i < msg.length; i++) {
			obj.push(msg[i]);
		}
		self.socket.send(obj);
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
			self.workerWaiting(worker, true);
		break;
		case -1:
			self.workerDelete(identify, false);
		break;
		case -2:
			self.workerDelete(identify, true);
		break;
	}
};

Broker.prototype.workerService = function (identify) {
    var self = this;

    var worker = self.workers[identify];
    return worker.service.name;
};

Broker.prototype.requestQueue = function (service, msg) {
	var client = msg[0].toString();
	var rid = msg[4].toString();

	this.rmap[rid] = {
		client: client,
		rid: rid,
		interest: (new Date()).getTime() + HEARTBEAT_EXPIRY
	};
	service.requests.push(msg);

	this.serviceDispatch(service);
};

Broker.prototype.serviceDispatch = function (service) {
    // invalidate workers list according to heartbeats
    this.workerPurge();

    while (service.waiting.length && service.requests.length) {
        // get idle worker
        var identify = service.waiting.pop();
        var i;
        for (i = 0; i < this.waiting.length; i++) {
            if (identify !== this.waiting[i]) {
                continue;
            }
            this.waiting.splice(i, 1);
            break;
        }

		var worker = this.workers[identify];

        // get next message
        var nextMsg = service.requests.pop();
		var rid = nextMsg[4].toString();

		if (!this.rmap[rid]) {
			this.workerWaiting(worker);
			continue;
		}

		var rme = this.rmap[rid];
		rme.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
		rme.workerId = identify;
		worker.rid = rid;
        
        var obj = [identify, MDP.WORKER, MDP.W_REQUEST, rme.client, ''];
		for (i = 4; i < nextMsg.length; i++) {
            obj.push(nextMsg[i]);
        }
        this.socket.send(obj);

        obj = [rme.client, MDP.CLIENT, MDP.W_HEARTBEAT, '', rid];
        this.socket.send(obj);
		// debug("Dispatched: workerId = %s, rid = %s", identify, rid);
    }
    debug('serviceDispatch: requests=%s, idle=%s', service.requests.length, service.waiting.length);
};

Broker.prototype.workerPurge = function () {
    var self = this;

	Object.keys(this.workers).every(function(identify) {
		var worker = self.workers[identify];
		if ((new Date()).getTime() >= worker.expiry) {
			self.workerDelete(identify, true);
		}
		return true;
	});
};

Broker.prototype.requestPurge = function () {
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
    this.waiting.push(worker.identify);
    worker.service.waiting.push(worker.identify);

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

Broker.prototype.workerRequire = function (identify) {
    if (identify in this.workers) {
        return this.workers[identify];
    }

	var worker = {
		identify: identify
	};
	this.workers[identify] = worker;
	return worker;
};

Broker.prototype.workerDelete = function (identify, disconnect) {
    var self = this;

    debug('workerDelete \'%s\'', identify, disconnect);

    if (!(identify in self.workers)) {
        return;
    }

    var i;
    var worker = self.workers[identify];
    if (disconnect) {
        self.socket.send([identify, MDP.WORKER, MDP.W_DISCONNECT]);
    }

	if (worker.rid) {
		this.requestDelete(worker.rid);
	}
	delete worker.rid;

    // remove worker from service's waiting list
    if (worker.service) {
        for (i = 0; i < worker.service.waiting.length; i++) {
            if (identify !== worker.service.waiting[i]) {
                continue;
            }
            worker.service.waiting.splice(i, 1);
            break;
        }
        worker.service.workers--;
    }

    // remove worker from broker's waiting list
    for (i = 0; i < self.waiting.length; i++) {
        if (identify !== self.waiting[i]) {
            continue;
        }
        self.waiting.splice(i, 1);
        break;
    }
    
    // remove worker from broker's workers list
    delete self.workers[identify];
};

Broker.prototype.serviceRequire = function (name) {
    var self = this;

    if (name in self.services) {
        return self.services[name];
    }

    var service = {
        name: name,
        requests: [],
        waiting: [],
        workers: 0
    };
    
    self.services[name] = service;

    return service;
};

module.exports = Broker;
