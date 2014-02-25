zmq-omdp
========

ZeroMQ Obsessive Majordomo Protocol: enhanced version of ZeroMQ Majordomo Protocol (MDP) v0.2 protocol for Node.JS.

####Credits
Based on https://github.com/nuh-temp/zmq-mdp2 project

Thanks to nuh-temp <nuh.temp@gmail.com> or genbit <sergey.genbit@gmail.com>

#### Features
* Client heartbeating for active requests. Allows Workers to dected whenever Clients disconnect or lose interest in some request. This feature is very useful to stop long-running partial requests (i.e data streaming) allowing Worker to be requeued by the Broker for new tasks.

#### Roadmap
* Add authentication support through [zmq-zap](https://github.com/msealand/zmq-zap.node) ZeroMQ ZAP to trust Clients and Workers.
