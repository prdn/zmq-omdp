var test = require('tape');
var omdp = require('../');

var location = 'inproc://#1';

test('echo server (partial/final)', function(t) {
	t.plan(104);

	var chunk = 'foo';

	var worker = new omdp.Worker(location, 'echo');
	worker.on('request', function (inp, res) {
		inp = String(inp);
		t.equal(inp, chunk, 'Worker input is equal to expected');
		for (var i = 0; i < 100; i++) {
			res.write(inp + i);
		}
		res.end(inp + 'FINAL' + (++i)); 
	});

	var client = new omdp.Client(location);

	worker.start();
	client.start();

	var broker = new omdp.Broker(location);
	broker.start(function() {
		t.pass('Broker callback was called');
	});

	function stop() {
		worker.stop();
		client.stop();
		broker.stop();
	}

	(function() {
		var repIx = 0;
		var d1 = new Date();
		client.request(
			'echo', chunk,
			function(data) {
				t.equal(String(data), chunk + (repIx++), 'Worker output PARTIAL (' + (repIx - 1) + ')');
			}, 
			function(err, data) {
				t.equal(String(data), chunk + 'FINAL' + (++repIx), 'Worker output FINAL (' + (repIx - 1) + ')');
				t.equal(true, true, (new Date() - d1) + ' milliseconds');
				stop();
			}
		);
	})();
});
