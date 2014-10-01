zmq-omdp
========

ZeroMQ Obsessive Majordomo Protocol: microservices framework for NodeJS - reliable and extensible service-oriented request-reply inspired by [ZeroMQ Majordomo Protocol (MDP) v0.2](http://rfc.zeromq.org/spec:7). 

#### Structure
* Worker : receives requests, does something and replies. A worker offers a service, should be a functionality as atomic as possible
* Client : creates, pushes requests and waits for results (if needed). A request always includes a service and a payload/data for the Worker
* Broker : handles requests queueing and routing

### API

All data sent through the API should be data strings.

#### `omdp.Worker(socket_str, service_name)`

Worker receives `"request"` events that contain 2 arguments:

* `data` - value sent by a client for this request.
* `reply` - extended writable stream to send data to the client.

`reply` writable stream exposes also following methods:

* `write` - sends partial data to the client (triggers partial callback). It is used internally to implement writable streams.
* `end` - sends last data to the client (triggers final callback) and completes/closes current request. Use this method for single-reply requests.
* `heartbeat` - forces sending heartbeat to the broker and client
* `active` - returns (boolean) the status of the request. A request becomes inactive when the worker disconnects from the broker or it is discarded by the client or the client disconnects from the broker. This is useful for long running tasks and Worker can monitor whether or not continue processing a request. 
* `closed` - returns (boolean) if the request has been closed, so the worker cannot push more data to the client.

````
worker.on('request', function(data, reply) {
  fs.createReadStream(data).pipe(reply);
});

// or
worker.on('request', function(data, reply) {
	for (var i = 0; i < 1000; i++) {
	  res.write('PARTIAL DATA ' + i);
	}
	res.end('FINAL DATA');
});
````

Take note: due to the framing protocol of `zmq` only the data supplied to `response.end(data)` will be given to the client's final callback.

#### `omdp.Client(socket_str)`

Clients may make simple requests using `client.request(...)` with 5 arguments.

* `serviceName` - name of the service we wish to connect to
* `data` - data to give to the service
* `partialCallback(data)` - called whenever the request does not end but emits data
* `finalCallback(err, data)` - called when the request will emit no more data
* `opts` - options object for the request

````
client.request('echo', 'data', function (data) {
  // frames sent prior to final frame
  console.log('partial data', data);
}, function (err, data) {
  // this is the final frame sent
  console.log('final data', data);
}, { timeout: 5000 });
````

Clients may also make streaming request using `client.requestStream()` with 3 arguments.

* `serviceName`
* `data`
* `opts`

````
client.requestStream('echo', 'data', { timeout: -1 }).pipe(process.stdout);
````

##### Request options
* `timeout` : default 60000 (60 seconds). Set -1 to disable

#### `omdp.Broker(socket_str)`

Simply starts up a broker.

Take note: when using a `inproc` socket the broker *must* become active before any queued messages.

#### Data encoding / decoding

`omdp.Worker` and `omdp.Client` have two methods, `encode` and `decode`, that can be overridden to handle custom messages formats (i.e JSON or XML).

`.encode(...)` with 1 argument. Must return a string.

* `data` - data to encode

`.decode(...)` with 1 argument. Returns a custom message representation. 

* `data` - data string to decode

##### Builtin JSON support

`omdp.JSONWorker` and `omdp.JSONClient` offer a builtin support for encoding / decoding JSON messages. 

### Protocol

#### Benefits
* Reliable request / reply protocol
* Scalability
* Multi-Worker : infinite services and infinite workers for each service
* Multi-Client : infinite clients
* Multi-Broker : infinite brokers to avoid bottlenecks and improve network reliability

#### Features
* Compatibility with MDP protocol v0.2 .
* Support for partial replies.
* Client multi-request support.
* Client heartbeating for active requests. Allows Workers to dected whenever Clients disconnect or lose interest in some request. This feature is very useful to stop long-running partial requests (i.e data streaming) allowing Worker to be requeued by the Broker for new tasks.
* Worker / Broker heartbeating for active / in-progress requests. Allow Clients to detect whenever a Worker quits or dies while processing a Client request.

#### Specification (good for RFC)
* Worker <-> Broker heartbeating.
* Broker MAY track Worker/Client/Request relation.
* Client MAY send heartbeat for active request. If the request is being processed by Worker, Broker forwards heartbeat to Worker. 
* Worker MAY decide to stop an inactive Request (tracks liveness for Request).
* Client MAY assign a timeout to a Request.
* Broker MAY decide to drop expired Requests (client timeout reached).
* Worker SHALL NOT send more W_REPLY (for a Request) after sending first W_REPLY message.
* Broker SHALL force disconnect Broker if any error occurs.

#### Roadmap
* Add authentication support through [zmq-zap](https://github.com/msealand/zmq-zap.node) ZeroMQ ZAP to trust Clients and Workers.

#### Follow me

* Fincluster - cloud financial platform : [fincluster](http://fincluster.com) /  [@fincluster](https://twitter.com/fincluster)
* My personal blog : [ardoino.com](http://ardoino.com) / [@paoloardoino](https://twitter.com/paoloardoino)

#### Credits
Based on https://github.com/nuh-temp/zmq-mdp2 project
