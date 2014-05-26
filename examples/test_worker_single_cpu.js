var cluster = require('cluster');
var Worker = require('./../index').Worker;
var numCPUs = require('os').cpus().length;

for (var i = 0; i < 8; i++) {
	(function() {
		var worker = new Worker('tcp://localhost:5555', 'echo');
		worker.start();

		function go(inp, rep) {

			function partial() {
				if (!rep.active()) {
					final();
					return;
				}
				rep.write("REPLY_PARTIAL-" + rep.rid + '-' + (new Date().getTime()));
			}

			function final() {
				clearInterval(rtmo);
				if (!rep.active()) {
					console.log("REQ INACTIVE");
					return;
				}
				console.log("RESPONSE REQ");
				rep.end("REPLY-" + rep.rid + '-' + (new Date().getTime()));
			}

			if (inp.partial) {
				var rtmo = setInterval(function() {
					partial();	
				}, 250);

				setTimeout(function() {
					final();
				}, 15000);
			} else {
				setTimeout(function() {
					final();
				}, 1000);
			}
		}

		worker.on('request', function(inp, rep) {
			console.log("RECEIVED REQ", inp);
			go(inp, rep);	
		});
	})();
}
