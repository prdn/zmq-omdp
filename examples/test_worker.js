var cluster = require('cluster');
var Worker = require('./../index').Worker;

if (cluster.isMaster) {
	for (var i = 0; i < 4; i++) {
		cluster.fork();
	}
	cluster.on('exit', function(worker, code, signal) {
		console.log('worker ' + worker.process.pid + ' died');
	});
} else {

	(function() {
		var worker = new Worker('tcp://localhost:5555', 'echo');
			worker.start();

		function go(inp, rep) {

			console.log(inp);

			function partial() {
				if (!rep.active()) {
					final();
					return;
				}
				rep.write("prova-" + rep.rid + '-' + (new Date().getTime()));
			}

			function final() {
				clearInterval(rtmo);
				if (!rep.active()) {
					return;
				}
				rep.end("prova-end");
			}

			if (inp.spot) {
				setTimeout(function() {
					final();
				}, 1000);
			} else {
				var rtmo = setInterval(function() {
					partial();	
				}, 250);

				setTimeout(function() {
					final();
				}, 100000);
			}
		}

		worker.on('request', function(inp, rep) {
			go(inp, rep);	
		});
	})();
}
