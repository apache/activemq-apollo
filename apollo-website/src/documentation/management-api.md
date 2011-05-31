# Apollo ${project_version} Management API

{:toc:2-5}

## Overview

Apollo's REST API runs on port 61680 or via SSL on port 61681. If your
running an Apollo broker on your local machine, you could access the
API at the following HTTP URLs:

    http://localhost:61680
    https://localhost:61681

For all of the rest of this document, we will be leaving off that part,
since it is the same for every API call.

### Authentication

The broker requires all requests against the management API to supply
user credentials which have administration privileges.

The user credentials should be supplied using via HTTP basic
authentication. Example:

    $ curl -u "admin:password" http://localhost:61680/broker

### JSON Representation

The API routes are intended to be access programmatically as JSON
services but they also provide an HTML representation so that the API
services can easily be browsed using a standard web browser.

You must either set the HTTP `Accept` header to `application/json` or
append `.json` to the URL to get the JSON representation of the data. 
Example:

    $ curl -H "Accept: application/json" -u "admin:password" \
    http://localhost:61680/broker

    $ curl -u "admin:password" http://localhost:61680/broker.json

### Working with Tabular Results

Many of the resource routes provided by the broker implement
a selectable paged tabular interface.  A good example of such
a resource route is the connections list.  It's route is:

    /broker/connections

Example:

    $ curl -u "admin:password" \
    'http://localhost:61680/broker/connections.json'
    [
      [
        "*"
      ],
      {
        "id":"4",
        "state":"STARTED",
        "state_since":1306848325102,
        "read_counter":103,
        "write_counter":239110628,
        "transport":"tcp",
        "protocol":"stomp",
        "remote_address":"/127.0.0.1:61775",
        "protocol_version":"1.0",
        "user":"admin",
        "waiting_on":"client request",
        "subscription_count":1
      },
      {
        "id":"5",
        "state":"STARTED",
        "state_since":1306848325102,
        "read_counter":227739229,
        "write_counter":113,
        "transport":"tcp",
        "protocol":"stomp",
        "remote_address":"/127.0.0.1:61776",
        "protocol_version":"1.0",
        "user":"admin",
        "waiting_on":"blocked sending to: org.apache.activemq.apollo.broker.Queue$$anon$1@13765e9b",
        "subscription_count":0
      }
    ]

The results are an array of records with the first record acting as a header
records describing the fields selected. The `*` field means all the record's
fields were selected. To narrow down the selected fields you can add
multiple `f` query parameters to pick the fields you want to retrieve.

Example:

    $ curl -u "admin:password" \
    'http://localhost:61680/broker/connections.json?f=id&f=read_counter'
    [
      [
        "id",
        "read_counter"
      ],
      [
        "7",
        110733109
      ],
      [
        "6",
        103
      ]
    ]

If you want to narrow down the records which get selected, you can set a `q`
query parameter to SQL 92 style where clause which uses the record's fields
to filter down the selected records.

For example to only view local connection, you would want to use a where
clause like `remote_address LIKE "/127.0.0.01:%"` which to execute with
`curl` you would run:

    curl -u "admin:password" \
    'http://localhost:61680/broker/connections.json?q=remote_address%20LIKE%20"/127.0.0.1:%"'

The records are paged. The default page size is 100, so only the first 100
records will be displayed. If you want to view subsequent results, you must
set the `p` query parameter to the page you wish to access. You can change
the page size by setting the `ps` query parameter.


### Broker Management

The route for managing the broker is:

    /broker

Doing a GET against it will provide information about the broker's

  * Version
  * Running State
  * Virtual Hosts
  * Connectors
  * Connections
  
Example:

    $ curl -u "admin:password" \
    http://localhost:61680/broker.json

Results in a [Broker Status](./api/apollo-dto/org/apache/activemq/apollo/dto/BrokerStatusDTO.html) :

{pygmentize:: js}
{
  "version":"${project_version}",
  "id":"default",
  "state":"STARTED",
  "state_since":1305388053086,
  "current_time":1305388817653,
  "virtual_hosts":["localhost"],
  "connectors":[
    "stomps",
    "stomp"
  ],
  "connections":[
    {"id":24,"label":"/127.0.0.1:52603"},
    {"id":25,"label":"/127.0.0.1:52604"}
  ],
}
{pygmentize}

### Connector Management

The route for getting a tabular list of connectors is:

    /broker/connectors

The route for managing a connector is:

    /broker/connectors/:id

Example:

    $ curl -u "admin:password"
    http://localhost:61680/broker/connectors/stomp.json

Results in a [Connector Status](./api/apollo-dto/org/apache/activemq/apollo/dto/ConnectorStatusDTO.html):

{pygmentize:: js}
{
  "id":"stomp",
  "state":"STARTED",
  "state_since":1305553109899,
  "accepted":6,
  "connected":2
}
{pygmentize}

To stop a connector send a POST to:

    /broker/connectors/:id/action/stop

Example:

    curl -X POST -u "admin:password" \
    http://localhost:61680/broker/connectors/stomp/action/stop.json

To start a stopped a connector send a POST to:

    /broker/connectors/:id/action/start

Example:

    curl -X POST -u "admin:password" \
    http://localhost:61680/broker/connectors/stomp/action/start.json

### Connection Management

The route for getting a tabular list of connections is:

    /broker/connections

The route for managing a single connection is:

    /broker/connections/:id

Example:

    $ curl -u "admin:password" \
    http://localhost:61680/broker/connections/5.json

Results in a [Connection Status](./api/apollo-dto/org/apache/activemq/apollo/dto/ConnectionStatusDTO.html):

{pygmentize:: js}
{
  "id":"5",
  "state":"STARTED",
  "state_since":1305553686946,
  "read_counter":1401476017,
  "write_counter":99,
  "transport":"tcp",
  "protocol":"stomp",
  "remote_address":"/127.0.0.1:52638",
  "protocol_version":"1.0",
  "user":"admin",
  "waiting_on":"client request",
  "subscription_count":0
}
{pygmentize}

To shutdown a connection send a POST to:

    /broker/connections/:id/action/shutdown

Example:

    curl -X POST -u "admin:password" \
    http://localhost:61680/broker/connections/5/action/shutdown.json


### Virtual Host Management

The route for managing a virtual host is:

    /broker/virtual-hosts/:name

Where `:name` is the id of a virtual host configured in the broker.
Example:

    $ curl -u "admin:password" \
    http://localhost:61680/broker/virtual-hosts/localhost.json

Results in a [Virtual Host Status](./api/apollo-dto/org/apache/activemq/apollo/dto/VirtualHostStatusDTO.html):

{pygmentize:: js}
{
  "id":"localhost",
  "state":"STARTED",
  "state_since":1305390871786,
  "topics":[
    "item.prices",
    "inventory.level"
  ],
  "queues":[
    "orders.req",
    "orders.res",
  ],
  "store":true
}
{pygmentize}

#### Virtual Host Store Management

The route for managing a virtual host's Store is:

    /broker/virtual-hosts/:name/store

Where `:name` is the id of a virtual host configured in the broker.

Example:

    $ curl -u "admin:password" \
    http://localhost:61680/broker/virtual-hosts/localhost/store.json

Results in a [Store Status](./api/apollo-dto/org/apache/activemq/apollo/dto/StoreStatusDTO.html):

{pygmentize:: js}
{
  "@class":"org.apache.activemq.apollo.broker.store.bdb.dto.BDBStoreStatusDTO",
  "state":"STARTED",
  "state_since":1305554120954,
  "canceled_message_counter":87927,
  "flushed_message_counter":28576,
  "canceled_enqueue_counter":87927,
  "flushed_enqueue_counter":28576,
  "message_load_latency":{
    "count":0,
    "total":0,
    "max":0,
    "min":0
  },
  "flush_latency":{
    "count":0,
    "total":0,
    "max":0,
    "min":0
  },
  "journal_append_latency":null,
  "index_update_latency":null,
  "message_load_batch_size":{
    "count":0,
    "total":0,
    "max":-2147483648,
    "min":2147483647
  },
  "pending_stores":0
}
{pygmentize}

#### Queue Management

The route for getting a tabular list of queues is:

    /broker/virtual-hosts/:name/queues

The route for managing a virtual host's Queue is:

    /broker/virtual-hosts/:name/queues/:qid

Where `:name` is the id of a virtual host configured in the broker and `:qid` is the id
of the queue.

Example:

    $ curl -u "admin:password" \
    http://localhost:61680/broker/virtual-hosts/localhost/queues/orders.req.json

Results in a [Queue Status](./api/apollo-dto/org/apache/activemq/apollo/dto/QueueStatusDTO.html):

{pygmentize:: js}
{
   "id":"orders.req",
   "config":{
      "id":null,
      "unified":null,
      "producer_buffer":null,
      "queue_buffer":null,
      "consumer_buffer":null,
      "persistent":null,
      "swap":null,
      "swap_range_size":null,
      "acl":null
   },
   "binding":{
      "@class":"org.apache.activemq.apollo.dto.QueueDestinationDTO",
      "path":[
         "orders",
         "req"
      ]
   },
   "metrics":{
      "enqueue_item_counter":72292,
      "enqueue_size_counter":79943846,
      "enqueue_ts":1306433477839,
      "dequeue_item_counter":72144,
      "dequeue_size_counter":79780158,
      "dequeue_ts":1306433477839,
      "nack_item_counter":0,
      "nack_size_counter":0,
      "nack_ts":1306433476813,
      "queue_size":163688,
      "queue_items":148,
      "swapped_in_size":0,
      "swapped_in_items":0,
      "swapping_in_size":0,
      "swapping_out_size":0,
      "swapped_in_size_max":32768,
      "swap_out_item_counter":148,
      "swap_out_size_counter":163688,
      "swap_in_item_counter":0,
      "swap_in_size_counter":0
   },
   "entries":[
   ],
   "producers":[
   ],
   "consumers":[
   ]
}
{pygmentize}


#### Topic Management

The route for getting a tabular list of queues is:

    /broker/virtual-hosts/:name/topics

The route for managing a virtual host's Topic is:

    /broker/virtual-hosts/:name/topics/:tid

Where `:name` is the id of a virtual host configured in the broker and `:tid` is the id
of the topic.

Example:

    $ curl -u "admin:password" \
    http://localhost:61680/broker/virtual-hosts/localhost/topics/item.prices.json

Results in a [Topic Status](./api/apollo-dto/org/apache/activemq/apollo/dto/TopicStatusDTO.html):

{pygmentize:: js}
{
  "id":"item.prices",
  "config":{
    "id":null,
    "slow_consumer_policy":null,
    "acl":null
  },
  "producers":[
    {
      "kind":"connection",
      "id":"3",
      "label":"/127.0.0.1:52772"
    }
  ],
  "consumers":[
    {
      "kind":"connection",
      "id":"4",
      "label":"/127.0.0.1:52773"
    }
  ],
  "dsubs":[
     "mysubname"
  ]
}
{pygmentize}


#### Durable Subscription Management

The route for getting a tabular list of durable subscriptions is:

    /broker/virtual-hosts/:name/dsubs

The route for managing a virtual host's durable subscription is:

    /broker/virtual-hosts/:name/dsubs/:sub

Where `:name` is the id of a virtual host configured in the broker and `:sub` is the id
of the durable subscription.

Example:

    $ curl -u "admin:password" \
    http://localhost:61680/broker/virtual-hosts/localhost/dsubs/mysub.json

Results in a [Queue Status](./api/apollo-dto/org/apache/activemq/apollo/dto/QueueStatusDTO.html):


### Getting the Broker's Configuration

To get current runtime configuration of the broker GET:

    /broker/config/runtime

Example:

    $ curl -u "admin:password" \
    http://localhost:61680/broker/config/runtime.json

Results in a [Broker Configuration](./api/apollo-dto/org/apache/activemq/apollo/dto/BrokerDTO.html):

{pygmentize:: js}
{
  "notes":"\n    The default configuration with tls/ssl enabled.\n  ",
  "virtual_hosts":[{
    "id":"apollo-a",
    "enabled":null,
    "host_names":["localhost"],
    "store":{
      "@class":"org.apache.activemq.apollo.broker.store.bdb.dto.BDBStoreDTO",
      "flush_delay":null,
      "directory":"/Users/chirino/opt/apollo-a/data",
      "read_threads":null,
      "zero_copy":null
    },
    "auto_create_destinations":null,
    "purge_on_startup":null,
    "topics":[],
    "queues":[],
    "dsubs":[],
    "regroup_connections":null,
    "acl":{
      "connects":[
        {"allow":"admins","deny":null,"kind":null}
      ]
    },
    "authentication":{
      "enabled":false,
      "domain":null,
      "acl_principal_kinds":[],
      "user_principal_kinds":[]
    },
    "router":null,
    "log_category":null
  }],
  "connectors":[{
      "id":"stomp",
      "enabled":null,
      "bind":"tcp://0.0.0.0:61613",
      "protocol":null,
      "connection_limit":2000,
      "protocols":[],"acl":null
    },{
      "id":"stomps",
      "enabled":null,
      "bind":"tls://0.0.0.0:61614",
      "protocol":null,
      "connection_limit":2000,
      "protocols":[],
      "acl":null
    }],
  "client_address":null,
  "key_storage":{
    "file":"/opt/apollo-a/etc/keystore",
    "password":null,
    "key_password":null,
    "store_type":null,
    "trust_algorithm":null,
    "key_algorithm":null
  },
  "acl":{
    "admins":[{
      "allow":"admins",
      "deny":null,
      "kind":null
    }]
  },
  "web_admins":[{
    "bind":"http://127.0.0.1:61680"
  }],
  "authentication":{
    "enabled":null,
    "domain":"apollo",
    "acl_principal_kinds":[],
    "user_principal_kinds":[]
  },
  "log_category":{
    "console":"console",
    "audit":"audit",
    "connection":"connection",
    "security":"security"
  },
  "sticky_dispatching":null
}
{pygmentize}
      
### Aggregate Queue Statistics

You can get aggregate queue statistics at either the broker or virtual host level by
using one of the following URL routes:

    /broker/queue-metrics
    /broker/virtual-hosts/:name/queue-metrics

Example:

    $ curl -u "admin:password" \
    http://localhost:61680/broker/queue-metrics.json

Results in an [Aggregate of Queue Metrics](./api/apollo-dto/org/apache/activemq/apollo/dto/AggregateQueueMetricsDTO.html):

{pygmentize:: js}
{
  "enqueue_item_counter":0,
  "enqueue_size_counter":0,
  "enqueue_ts":0,
  "dequeue_item_counter":0,
  "dequeue_size_counter":0,
  "dequeue_ts":0,
  "nack_item_counter":0,
  "nack_size_counter":0,
  "nack_ts":0,
  "queue_size":0,
  "queue_items":0,
  "swapped_in_size":0,
  "swapped_in_items":0,
  "swapping_in_size":0,
  "swapping_out_size":0,
  "swapped_in_size_max":0,
  "swap_out_item_counter":0,
  "swap_out_size_counter":0,
  "swap_in_item_counter":0,
  "swap_in_size_counter":0,
  "queues":0
}
{pygmentize}
