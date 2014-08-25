var cluster = require('cluster');
var numCPUs = require('os').cpus().length;
var ZOM = require('./../../index');

var Worker = ZOM.Worker;

if (cluster.isMaster) {
	for (var i = 0; i < 1/*numCPUs*/; i++) {
		cluster.fork();
	}
	cluster.on('exit', function(worker, code, signal) {
		console.log('worker ' + worker.process.pid + ' died');
	});
} else {
	var workerID = cluster.worker.workerID;

	for (var i = 0; i < numCPUs; i++) {
		(function(i) {
			var wname = "W" + workerID + "/" + i;
			console.log("CREATING WORKER " + wname);

			function genReply(type) {
				return 'bar_' + (type || 'unk') + '-' + (new Date().getTime());
			}

			var worker = new Worker('tcp://localhost:55555', 'echo');
				worker.start();

			worker.on('error', function(e) {
				console.log('ERROR', e);
			});

			function go(inp, rep) {

				function partial() {
					rep.heartbeat(); // optional
					if (!rep.active()) {
						final();
						return;
					}
					rep.write(genReply('partial'));
				}

				function final() {
					clearInterval(rtmo);
					if (!rep.active()) {
						console.log("REQ INACTIVE");
					}
					if (rep.closed()) {
						console.log("REQ ALREADY CLOSED");
						return;
					}
					rep.end(genReply('final'));
				}

				var rtmo = setInterval(function() {
					partial();	
				}, 250);

				setTimeout(function() {
					final();
				}, 10000);
			}

			worker.on('request', function(inp, rep) {
				console.log("WORKER-" + wname + " RECV REQ");
				go(inp, rep);	
			});
		})(i);
	}
}
