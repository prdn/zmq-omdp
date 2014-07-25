var ZOM = require('./../index');

var mode = process.argv[2];
var Client;

if (mode == 'json') {
	Client = ZOM.JSONClient;
} else {
	Client = ZOM.Client;
}

var client = new Client('tcp://localhost:55555');
client.start();
client.on('error', function() {
	console.log("ERROR", arguments);
});

var rcnt = 0;

for (var i = 0; i < 6; i++) {
	(function(i) {
		client.requestStream(
			'echo', 'bar',
		).pipe(process.stdout);
	})(i);
}
