App = Em.Application.create({
  ready: function() {
    var self = this;
//    setInterval(function() {
//      if( App.LoginController.get('is_logged_in') ) {
//        self.refresh();
//      }
//    }, 2000);
    App.LoginController.refresh();
    this._super();
  },
  
  refresh: function() {
    App.BrokerController.refresh();
    App.VirtualHostController.refresh();
    App.ConnectorController.refresh();
  },

  default_error_handler:function(xhr, status, thrown) {
    if( xhr.status == 0 ) {
      App.BrokerController.set("offline", true);
    } else {
      App.BrokerController.set("offline", false);
      if( xhr.status == 401 ) {
        Bootstrap.AlertMessage.create({
          type:"warning",
          message:"Action not authorized."
        }).appendTo("#notifications")
        App.LoginController.refresh();
      } else {
        Bootstrap.AlertMessage.create({
          type:"error",
          message:xhr.status+": "+thrown
        }).appendTo("#notifications")
      }
    }
  },

  ajax:function(type, path, success, error) {
    if( !error ) {
      error = this.default_error_handler;
    }
    $.ajax({
      type: type,
      url: "../api/json"+path,
      headers: {
        AuthPrompt:'false',
      },
      dataType: 'json',
      success: function(data, textStatus, jqXHR){
        App.BrokerController.set("offline", false);
        if( success ) {
          success(data, textStatus, jqXHR)
        }
      },
      error: error,
    });
  },
});

App.LoginController = Em.Controller.create({
  
  username:"",
  password:"",
  
  login: function() {
    var username = this.get('username');
    var password = this.get('password');
    this.set('username', "")
    this.set('password', "")
    var self = this;
    $.ajax({
      type: "POST",
      url: "../api/json/session/signin",
      headers: {
        AuthPrompt:'false',
      },
      data:{
        username: username,
        password: password
      },
      dataType: 'json',
      success: function(data){
        if( data ) {
          self.refresh();
          App.refresh();
        } else {
          self.set('content', null);
          Bootstrap.AlertMessage.create({
            type:"warning",
            message:"Username or password are invalid."
          }).appendTo("#notifications")
        }
      },
      error: function(xhr, status, thrown){
          self.set('content', null);
          Bootstrap.AlertMessage.create({
            type:"error",
            message:"Failure contacting server: "+status
          }).appendTo("#notifications")
      },
    });
  },

  logout: function() {
    App.ajax("GET", "/session/signout", function(data) {
      App.LoginController.set('content', data);
    });
    this.set('content', null);
  },
  
  user_name: function() {
    var content = this.get('content');
    if( content && content.length > 0) {
      var p = content.find(function(item) { return item.kind.indexOf("UserPrincipal") != -1; });
      if( p!=null ) {
        return p.name;
      }
    }
    return null;
  }.property('content'),

  is_logged_in: function() {
    var content = this.get('content');
    return content && content.length > 0;
  }.property('content'),
  
  refresh: function(clear) {
    var was_logged_in = this.get('is_logged_in')
    var kind = this.get('kind')
    App.ajax("GET", "/session/whoami", function(data) {
      App.LoginController.set('content', data);
      if( App.LoginController.get('is_logged_in') ) {
        App.refresh();
      }
    }, function(xhr, status, thrown) {
      App.LoginController.set('content', null);
    });
  }
})

App.broker = Ember.Object.create({});
App.BrokerController = Ember.Controller.create({
  offline:false,
  refresh: function() {
    App.ajax("GET", "/broker", function(json) {
      App.broker.setProperties(json);
      if( App.VirtualHostController.get('selected') == null && json.virtual_hosts.length > 0 ) {
        App.VirtualHostController.set('selected', json.virtual_hosts[0]);
        App.ConnectorController.set('selected', json.connectors[0]);
      }
    });
  }
});

App.connector = Ember.Object.create({});
App.ConnectorController = Em.ArrayController.create({
  selected:null,
  refresh: function() {
    var selected = this.get("selected")
    if( selected ) {
      App.ajax("GET", "/broker/connectors/"+selected, function(connector) {
        connector.state_date = new Date(connector.state_since);
        App.connector.setProperties(connector);
      });
    }
  }.observes("selected")
});


App.ConnectionsController = Ember. ArrayController.create({
  connectorBinding: "App.ConnectorController.selected",

  refresh: function(clear) {
    var connector = this.get('connector');
    if( !connector ) {
      App.ConnectionsController.set('content', []);
      return;
    }
    if( clear ) {
      App.ConnectionsController.set('content', []);
    }
    var kind = this.get('kind')
    var fields = ['id', 'remote_address', 'protocol', 'user', 'read_counter', 'write_counter', 'messages_received', 'messages_sent'];

    App.ajax("GET", "/broker/connections?q=connector='"+connector+"'&ps=10000&f="+fields.join("&f="), function(data) {
      App.ConnectionsController.set('content', data.rows);
    });
  }.observes("connector"),

  all_checked:false,
  check_all_toggle: function() {
    var all_checked= this.get("all_checked");
    this.get('content').forEach(function(item){
      item.set('checked', all_checked);
    });
  }.observes("all_checked"),

  remove: function() {
    var content = this.get('content');
    content.forEach(function(item){
      var checked = item.get('checked');
      if( checked ) {
        var id = item.get(0);
        App.ajax("DELETE", "/broker/connections/"+id, function(data) {
          App.ConnectionsController.refresh();
        });
      }
    });
  },

});

App.virtual_host = Ember.Object.create({});
App.virtual_host_store = Ember.Object.create({});
App.VirtualHostController = Em.ArrayController.create({
  tabs:["Queues","Topics","Durable Subs"],
  selected_tab:"Queues",
  selected:null,
  refresh: function() {
    var selected = this.get("selected")
    if( selected ) {
      App.ajax("GET", "/broker/virtual-hosts/"+selected, function(host) {
        App.virtual_host.setProperties(host);
        host.state_date = new Date(host.state_since);
        if( host.store ) {
          App.ajax("GET", "/broker/virtual-hosts/"+selected+"/store", function(store) {
            App.virtual_host_store.setProperties(store);
          });
        } 
      });
    }
  }.observes("selected")
});

App.DestinationsController = Ember. ArrayController.create({
  virtual_hostBinding: "App.VirtualHostController.selected",
  create_name: "",

  kind: function(){
    var s = App.VirtualHostController.get('selected_tab');
    if( s == "Queues" ) {
      return "queues";
    } else if( s == "Topics" ) {
      return "topics";
    } else if( s == "Durable Subs" ) {
      return "dsubs";
    }
  }.property("App.VirtualHostController.selected_tab"),

  kind_label: function() {
    var v = App.VirtualHostController.get('selected_tab')
    return v.substring(0, v.length-1);
  }.property("App.VirtualHostController.selected_tab"),

  refresh: function(clear) {
    var virtual_host = this.get('virtual_host');
    if( !virtual_host ) {
      App.DestinationsController.set('content', []);
      return;
    }
    if( clear ) {
      App.DestinationsController.set('content', []);
    }
    var kind = this.get('kind')
    var fields = ['id', 'metrics.queue_items', 'metrics.queue_size', 'metrics.producer_count', 'metrics.consumer_count'];
    App.ajax("GET", "/broker/virtual-hosts/"+virtual_host+"/"+kind+"?ps=10000&f="+fields.join("&f="), function(data) {
      App.DestinationsController.set('content', data.rows);
    });
  }.observes("virtual_host", "kind"),

  show_create_box: function(){
    var kind = this.get('kind');
    return kind == "queues" || kind == "topics";
  }.property("kind"),

  create: function() {
    var virtual_host = this.get('virtual_host');
    var kind = this.get('kind');
    var create_name = this.get('create_name');
    this.set('create_name', "");
    App.ajax("PUT", "/broker/virtual-hosts/"+virtual_host+"/"+kind+"/"+create_name, function(data) {
      App.DestinationsController.refresh();
    });
  },

  all_checked:false,

  check_all_toggle: function() {
    var all_checked= this.get("all_checked");
    this.get('content').forEach(function(item){
      item.set('checked', all_checked);
    });
  }.observes("all_checked"),

  remove: function() {
    var virtual_host = this.get('virtual_host');
    var kind = this.get('kind');
    var content = this.get('content');
    content.forEach(function(item){
      var checked = item.get('checked');
      if( checked ) {
        var name = item.get(0);
        App.ajax("DELETE", "/broker/virtual-hosts/"+virtual_host+"/"+kind+"/"+name, function(data) {
          App.DestinationsController.refresh();
        });
      }
    });
  },

  selected:null,
  select: function(event) {
    this.set("selected", event.context.get(0))
  },

});

App.destination = null;
App.DestinationController = Em.Controller.create({
  destinationBinding:"App.destination",
  selectedBinding:"App.DestinationsController.selected",

  refresh: function() {
    var selected = this.get("selected")
    if( selected==null ) {
      App.set('destination', null);
    } else {
      var virtual_host = App.DestinationsController.get("virtual_host");
      var kind = App.DestinationsController.get("kind");
      App.ajax("GET", "/broker/virtual-hosts/"+virtual_host+"/"+kind+"/"+selected+"?consumers=true&producers=true", function(data) {
        data.metrics.state_date = new Date(data.state_since);
        data.metrics.enqueue_date = new Date(data.metrics.enqueue_ts);
        data.metrics.dequeue_date = new Date(data.metrics.dequeue_ts);
        data.metrics.nak_date = new Date(data.metrics.nak_ts);
        data.metrics.expired_date = new Date(data.metrics.expired_ts);
        data.producers.forEach(function(value){
          value.enqueue_date = new Date(value.enqueue_ts);
        });
        data.consumers.forEach(function(value){
          value.enqueue_date = new Date(value.enqueue_ts);
        });
        App.set('destination', data);
      });
    }
  }.observes("selected"),
});

Ember.View.create({
  templateName: 'notifications',
}).appendTo("#notifications");

Ember.View.create({
  templateName: 'navbar',
}).appendTo("#navbar-holder");

Ember.View.create({
  templateName: 'content',
}).appendTo("#content-holder");

App.ApplicationController = Ember.Controller.extend();
App.ApplicationView = Ember.View.extend();
App.Router = Ember.Router.extend({
  root: Ember.Route.extend({
    index: Ember.Route.extend({
      route: "/",
      enter: function ( router ){
        console.log("The index sub-state was entered.");
      },
      //redirectsTo: "queues"
    }),
    virtual_hosts:  Ember.Route.extend({
      showShoe:  Ember.Route.transitionTo('shoes.shoe'),
      route: '/virtual-hosts',
      index:  Ember.Route.extend({
        route: '/',
        enter: function ( router ){
          console.log("The virtual-hosts state was entered.");
        }
      }),
      shoe:  Ember.Route.extend({
        route: '/:id',
        connectOutlets: function(router, ctx) {
        },
        deserialize:  function(router, context){
          return context.id; // todo decode
        },
        serialize:  function(router, context){
          return {
            id: context.id // todo encode
          }
        },
      })
    }),
  })
})
App.initialize();
