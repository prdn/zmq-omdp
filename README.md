zmq-omdp
========

ZeroMQ Obsessive Majordomo Protocol: enhanced version of [ZeroMQ Majordomo Protocol (MDP) v0.2](http://rfc.zeromq.org/spec:7) for Node.JS.

### API

All data sent through the API should be JSON serializable.

#### `omdp.Worker(socket_str, service_name)`

Workers recieve `"request"` events that contain 2 arguments.

* `data` - this is the value sent by a client for this request
* `response` - this is a writable stream to send data to the client

Take note: due to the framing protocol of `zmq` only the data supplied to `response.end(data)` will be given to the client's final callback.

````
worker.on('request', function (data, response) {
  fs.createReadStream(data).pipe(response);
});
````

#### `omdp.Client(socket_str)`

Clients may make simple requests using `client.request(...)` with 4 arguments.

* `serviceName` - name of the service we wish to connect to
* `data` - data to give to the service
* `partialCallback(data)` - called whenever the request does not end but emits data
* `finalCallback(err, data)` - called when the request will emit no more data

````
client.request('echo', 'data', function (data) {
  // frames sent prior to final frame
  console.log('partial data', data);
}, function (err, data) {
  // this is the final frame sent
  console.log('final data', data);
});
````

Clients may also make streaming request using `client.requestStream()` with 2 arguments.

* `serviceName`
* `data`

````
client.requestStream('echo', 'data').pipe(process.stdout);
````

#### `omdp.Broker(socket_str)`

Simply starts up a broker.

Take note: when using a `inproc` socket the broker *must* become active before any queued messages.

####Credits
Based on https://github.com/nuh-temp/zmq-mdp2 project

Thanks to nuh-temp <nuh.temp@gmail.com> or genbit <sergey.genbit@gmail.com>

#### Features
* Compatibility with MDP protocol v0.2 .
* Support for partial replies.
* Client multi-request support.
* Client heartbeating for active requests. Allows Workers to dected whenever Clients disconnect or lose interest in some request. This feature is very useful to stop long-running partial requests (i.e data streaming) allowing Worker to be requeued by the Broker for new tasks.
* Worker / Broker heartbeating for active / in-progress requests. Allow Clients to detect whenever a Worker quits or dies while processing a Client request.

#### Protocol (good for an RFC)
* Worker <-> Broker heartbeating.
* Broker MAY track Worker/Client/Request relation.
* Broker MAY notify Client on Request dispatch to Worker.
* Client MAY send heartbeat for active request. If the request is being processed by Worker, Broker forwards heartbeat to Worker. 
* Worker MAY decide to stop an inactive Request (tracks liveness for Request).
* Worker MAY send heartbeat for active request. Broker forwards heartbeat to Client.
* Client MAY assign a timeout to a Request.
* Broker MAY decide to drop expired Requests (client timeout reached).
* Client MAY decide to timeout an inactive Request (tracks liveness for Request).
* Worker SHALL NOT send more W_REPLY (for a Request) after sending first W_REPLY message.
* Broker SHALL force disconnect Broker if any error occurs.

#### Roadmap
* Add authentication support through [zmq-zap](https://github.com/msealand/zmq-zap.node) ZeroMQ ZAP to trust Clients and Workers.

#### Follow me

* Fincluster - cloud financial platform : [fincluster](http://fincluster.com) /  [@fincluster](https://twitter.com/fincluster)
* My personal blog : [ardoino.com](http://ardoino.com) / [@paoloardoino](https://twitter.com/paoloardoino)
