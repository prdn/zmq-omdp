var Client = require('./../index').Client;

var client = new Client('tcp://localhost:5555');
client.start();

var rcnt = 0;

for (var i = 0; i < 10; i++) {
	(function(i) {
		client.request(
			"echo", { partial: true, foo: 'bar' }, 
			function(data) {
				console.log("REPLY_PARTIAL", i);  
			}, function() {
				rcnt++;
				console.log("REPLY_FINAL", i, rcnt);
			}
		);
	})(i);
}
