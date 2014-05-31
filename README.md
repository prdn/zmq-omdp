zmq-omdp
========

ZeroMQ Obsessive Majordomo Protocol: enhanced version of [ZeroMQ Majordomo Protocol (MDP) v0.2](http://rfc.zeromq.org/spec:7) for Node.JS.

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
* Client <-> Broker heartbeating 
* Worker <-> Broker heartbeating
* Broker MAY track Worker/Client/Request relation
* Broker MAY flag disconnected/dead Client's requests as inactive  
* Broker MAY notify Worker about running Request status
* Worker MAY decide to stop an inactive Request
* Worker SHALL NOT send more W_REPLY (for a Request) after sending first W_REPLY message
* Broker SHALL force disconnect Broker if any error occurs

#### Roadmap
* Add authentication support through [zmq-zap](https://github.com/msealand/zmq-zap.node) ZeroMQ ZAP to trust Clients and Workers.

#### Follow me

* Fincluster - cloud financial platform : [fincluster](http://fincluster.com) /  [@fincluster](https://twitter.com/fincluster)
* My personal blog : [ardoino.com](http://ardoino.com) / [@paoloardoino](https://twitter.com/paoloardoino)
