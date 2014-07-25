var Client = require('./../index').Client;

var client = new Client('tcp://localhost:55555');
client.start();

var rcnt = 0;

for (var i = 0; i < 6; i++) {
	(function(i) {
		client.request(
			'echo', 'foo', 
			function(data) {
				console.log('REPLY_PARTIAL', i);  
			}, function(err, data) {
				rcnt++;
				console.log('REPLY_FINAL', i, rcnt, err, data);
			}, { timeout: 240000 }
		);
	})(i);
}
