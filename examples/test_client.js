var ZOM = require('./../index');

var mode = process.argv[2];
var Client;

console.log("MODE: " + (mode || 'default'));

if (mode == 'json') {
	Client = ZOM.JSONClient;
} else {
	Client = ZOM.Client;
}

var client = new Client('tcp://localhost:55555');
client.start();

client.on('error', function(e) {
	console.log('ERROR', e);
});

function genRequest() {
	var msg = 'foo' + (new Date().getTime());
	if (mode == 'json') {
		return { msg: msg };
	}
	return msg;
}

var rcnt = 0;

for (var i = 0; i < 6; i++) {
	(function(i) {
		client.request(
			'echo', genRequest(), 
			function(data) {
				console.log('REPLY_PARTIAL', i, data);  
			}, function(err, data) {
				rcnt++;
				console.log('REPLY_FINAL', i, rcnt, err, data);
			}, { timeout: 240000 }
		);
	})(i);
}
