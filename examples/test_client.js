var Client = require('./../index').Client;

var client = new Client('tcp://localhost:55555');
client.start();

var rcnt = 0;

for (var i = 0; i < 10; i++) {
	(function(i) {
		client.request(
			"echo", { foo: 'bar' }, 
			function() {},
			function(err, data) {
				console.log("REPLY", i, rcnt++, err, data);
			}, { timeout: 1 }
		);
	})(i);
}
