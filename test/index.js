var test = require('tape');
var omdp = require('../');

var location = 'inproc://#1';

test('test server (partial/final)', function(t) {
	t.plan(8);

	var chunk = 'foo';

	var worker = new omdp.Worker(location, 'test');
	worker.on('request', function(inp, res) {
		t.equal(inp, chunk, 'Worker input');
		for (var i = 0; i < 5; i++) {
			res.write(inp + i);
		}
		res.end(inp + 'FINAL' + (++i)); 
	});
	worker.start();

	var broker = new omdp.Broker(location);
	broker.start(function() {
		t.pass('Broker callback');
	});
	
	var client = new omdp.Client(location);
	client.start();

	function stop() {
		worker.stop();
		client.stop();
		broker.stop();
	}

	var repIx = 0;
	client.request(
		'test', chunk,
		function(data) {
			t.equal(data, chunk + (repIx++), 'Partial msg matches');
		}, 
		function(err, data) {
			t.equal(data, chunk + 'FINAL' + (++repIx), 'Final msg matches');
			stop();
		}
	);
});

test('test server (final/error)', function(t) {
	t.plan(2);

	var chunk = 'SOMETHING_FAILED';

	var worker = new omdp.Worker(location, 'test');
	worker.on('request', function(inp, res) {
		res.error(chunk); 
	});
	worker.start();

	var broker = new omdp.Broker(location);
	broker.start(function() {});
	
	var client = new omdp.Client(location);
	client.start();

	function stop() {
		worker.stop();
		client.stop();
		broker.stop();
	}

	client.request(
		'test', 'foo',
		function(data) {}, 
		function(err, data) {
			t.equal(err, chunk, 'Error msg matches');
			t.equal(data, null, 'Data is not defined');
			stop();
		}
	);
});

test('test server (JSON/final)', function(t) {
	t.plan(2);

	var chunk = { foo: 'bar' };

	var worker = new omdp.JSONWorker(location, 'test');
	worker.on('request', function(inp, res) {
		res.end(chunk); 
	});
	worker.start();

	var broker = new omdp.Broker(location);
	broker.start(function() {});
	
	var client = new omdp.JSONClient(location);
	client.start();

	function stop() {
		worker.stop();
		client.stop();
		broker.stop();
	}

	client.request(
		'test', 'foo',
		function(data) {}, 
		function(err, data) {
			t.equal(err, null, 'Error is null');
			t.deepEqual(data, chunk, 'Final message matches');
			stop();
		}
	);
});
