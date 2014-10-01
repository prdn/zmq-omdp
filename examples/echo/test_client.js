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
			function(data) {
				console.log('REPLY_PARTIAL', i, data);  
			}, function(err, data) {
				rcnt++;
				console.log('REPLY_FINAL', i, rcnt, err, data);
				clearInterval(htmo);
			}, { timeout: 60000 }
		);
		
		var htmo = setInterval(function() {
			req.heartbeat();	   
		}, 1000);

	})(i);
}
