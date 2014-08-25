var ZOM = require('./../../index');

var Client = ZOM.Client;

function genRequest() {
	return 'foo' + (new Date().getTime());
};

var client = new Client('tcp://localhost:55555');
	client.start();
client.on('error', function() {
	console.log("ERROR", arguments);
});

for (var i = 0; i < 6; i++) {
	(function(i) {
		client.requestStream(
			'echo', genRequest()
		).pipe(process.stdout);
	})(i);
}
