var cluster = require('cluster');
var omdp = require('../');

var chunk = 'foo',
probes = 100000;

if (cluster.isMaster) {
	for (var i = 0; i < 3; i++) {
		cluster.fork();
	}
	cluster.on('exit', function(worker, code, signal) {
		for (var id in cluster.workers) {
			cluster.workers[id].kill();
		}
	});
} else {
	var workerID = cluster.worker.workerID;
	switch (+workerID) {
		case 1:
			var broker = new omdp.Broker('tcp://*:55559');
			broker.start(function() {
				console.log("BROKER started");
			});
		break;
		case 2:
			var worker = new omdp.Worker('tcp://localhost:55559', 'echo');
			worker.on('request', function(inp, res) {
				console.log("WORKER req received");
				console.log("\tsending " + probes + " probes");
				for (var i = 0; i < probes; i++) {
					res.write(inp);
				}
				res.end(inp + 'FINAL');
			});
			worker.start();
		break;
		case 3:
			var client = new omdp.Client('tcp://localhost:55559');
			client.start();

			var d1 = new Date();
			client.request(
				'echo', chunk,
				function(data) {},
				function(err, data) {
					var dts = (new Date() - d1);
					console.log("CLIENT GOT answer", dts + " milliseconds. " + (probes / (dts / 1000)).toFixed(2) + " requests/sec.");
					client.stop();
					process.exit(-1);
				}
			);
		break;
	}
}
