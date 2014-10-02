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
			undefined, undefined, { timeout: 60000 }
		)
		.on('data', function(data) {
			console.log("PARTIAL", i, String(data));
		}).on('end', function() {
			console.log("END", i);
		})
		.on('error', function(err) {
			console.log("ERROR", err);
		});
		
		var htmo = setInterval(function() {
			req.heartbeat();	   
		}, 1000);

	})(i);
}
