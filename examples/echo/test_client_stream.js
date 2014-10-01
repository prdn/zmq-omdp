var ZOM = require('./../../index');

var Client = ZOM.Client;

function genRequest() {
	return 'foo' + (new Date().getTime());
};

var client = new Client('tcp://localhost:55555');
client.start();

client.on('error', function() {
	console.log("CLIENT ERROR", arguments);
});

for (var i = 0; i < 6; i++) {
	(function(i) {
		var req = client.requestStream(
			'echo', genRequest()
		).on('error', function() {
			console.log("REQ ERROR", arguments);
		});
		
		req.pipe(process.stdout);
	})(i);
}
