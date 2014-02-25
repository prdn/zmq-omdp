var Client = require('./../index').Client;

var client = new Client('tcp://localhost:5555');
client.start();

client.request("echo", { foo1: 'bar1' }, function() {
	console.log("PARTIAL");   
}, function() {
	console.log("FINAL");   
});
