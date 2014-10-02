var ZOM = require('./../../index');

var Client = ZOM.Client;

var client = new Client('tcp://localhost:55555');
client.start();

client.on('error', function(e) {
	console.log('ERROR', e);
});

function genRequest() {
	return 'foo' + (new Date().getTime());
};

var rcnt = 0;

for (var i = 0; i < 6; i++) {
	(function(i) {
		var req = client.request(
			'echo', genRequest(), 
			function(err, data) {
				console.log("PARTIAL", i, err, data);
			},
			function(err, data) {
				console.log("END", i, err, data);
			}, { timeout: 60000 }
		);
	})(i);
}
