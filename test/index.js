var test = require('tape');
var omdp = require('../');

var location = 'inproc://#1';

test('echo server', function (t) {
    t.plan(3);

    var chunk = 'jello, curled!';

    var worker = new omdp.Worker(location, 'echo');
    worker.on('request', function (inp, res) {
      inp = String(inp);
      t.equal(inp, chunk, 'Worker input is equal to expected');
      res.end(inp); 
    });

    var client = new omdp.Client(location);

    worker.start();
    client.start();

    setTimeout(function () {
      var broker = new omdp.Broker(location);
      broker.start(function () {
        t.pass('Broker callback was called')
        client.request('echo', chunk, function () {}, function (err, data) {
          t.equal(String(data), chunk, 'Worker output is equal to expected');
          worker.stop();
          client.stop();
          broker.stop();
        });
      });
    }, 100);
});
