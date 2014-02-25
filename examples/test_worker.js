var Worker = require('./../index').Worker;

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

	var rtmo = setInterval(function() {
		partial();	
	}, 250);

	setTimeout(function() {
		final();
	}, 100000);
}

worker.on('request', function(inp, rep) {
	go(inp, rep);	
});

