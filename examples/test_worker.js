var cluster = require('cluster');
var Worker = require('./../index').Worker;
var numCPUs = require('os').cpus().length;

if (cluster.isMaster) {
	for (var i = 0; i < numCPUs; i++) {
		cluster.fork();
	}
	cluster.on('exit', function(worker, code, signal) {
		console.log('worker ' + worker.process.pid + ' died');
	});
} else {

	(function() {
		var worker = new Worker('tcp://localhost:55555', 'echo');
		worker.start();

		worker.on('error', function(e) {
			throw new Error(e);
		});

		function go(inp, rep) {

			function partial() {
				rep.heartbeat();
				if (!rep.active()) {
					final();
					return;
				}
				rep.write("REPLY_PARTIAL-" + (new Date().getTime()));
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
				rep.end("REPLY_END-" + (new Date().getTime()));
			}

			if (inp.partial) {
				var rtmo = setInterval(function() {
					partial();	
				}, 250);

				setTimeout(function() {
					final();
				}, 30000);
			} else {
				setTimeout(function() {
					final();
				}, 1000);
			}
		}

		worker.on('request', function(inp, rep) {
			go(inp, rep);	
		});
	})();
}
