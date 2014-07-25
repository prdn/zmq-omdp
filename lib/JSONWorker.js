var Worker = require('./Worker');
var parser = require('./parsers').JSON;
var util = require('util');

function JSONWorker() {
	Worker.apply(this, arguments);
};
util.inherits(JSONWorker, Worker);

JSONWorker.prototype.encode = parser.encode;
JSONWorker.prototype.decode = parser.decode;

module.exports = JSONWorker;
