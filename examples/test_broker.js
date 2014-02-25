var Broker = require('./../index').Broker;

var broker = new Broker("tcp://*:5555");
broker.start(function(){});
