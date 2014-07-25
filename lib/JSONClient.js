var Client = require('./Client');
var parser = require('./parsers').JSON;
var util = require('util');

function JSONClient() {
	Client.apply(this, arguments);
};
util.inherits(JSONClient, Client);

JSONClient.prototype.encode = parser.encode;
JSONClient.prototype.decode = parser.decode;

module.exports = JSONClient;
