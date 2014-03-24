var Client = require('./../index').Client;

var client = new Client('tcp://localhost:5555');
client.start();

var rcnt = 0;

for (var i = 0; i < 1; i++) {
	(function(i) {
		client.request("echo", { spot: false }, function() {
			console.log("PARTIAL", i);  
		}, function() {
			rcnt++;
			console.log("FINAL", i, rcnt);
		});
	})(i);
}
