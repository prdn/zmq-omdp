var Client = require('./../index').Client;

var client = new Client('tcp://localhost:55555');
client.start();
client.on('error', function() {
	console.log("ERROR", arguments);
});

var rcnt = 0;

for (var i = 0; i < 6; i++) {
	(function(i) {
		client.requestStream(
			"echo", { partial: true, foo: 'bar' }
		).pipe(process.stdout);
	})(i);
}
