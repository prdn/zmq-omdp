var cluster = require('cluster');
var omdp = require('../');

var chunk = 'foo';

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
	if (workerID == 1) {
		setTimeout(function() {
			var broker = new omdp.Broker('tcp://*:55559');
			broker.start(function() {});
		}, 100);

	} else if (workerID == 2) {
		var worker = new omdp.Worker('tcp://localhost:55559', 'echo');
		worker.on('request', function(inp, res) {
			inp = String(inp);
			for (var i = 0; i < 1000; i++) {
				res.write(inp);
			}
			res.end(inp + 'FINAL'); 
		});
		worker.start();

	} else if (workerID == 3) {
		var test = require('tape');
		test('echo server (partial/final)', function(t) {
			var client = new omdp.Client('tcp://localhost:55559');
			client.start();

			var repIx = 0;
			var d1 = new Date();
			client.request(
				'echo', chunk,
				function(data) {
					t.equal(String(data), chunk, 'Worker output PARTIAL');
				}, 
				function(err, data) {
					t.equal(String(data), chunk + 'FINAL', 'Worker output FINAL');
					t.equal(true, true, (new Date() - d1) + ' milliseconds');
					client.stop();
					t.end();
					process.exit(-1);
				}
			);
		});
	}
}
