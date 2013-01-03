App = Em.Application.create({
  ready: function() {
    var self = this;
    setInterval(function() {
      if( App.LoginController.get('is_logged_in') ) {
        self.refresh();
      }
    }, 2000);
    App.LoginController.refresh();
    this._super();
  },
  
  refresh: function() {
    App.BrokerController.refresh();
    App.VirtualHostController.refresh();
  },

  default_error_handler:function(xhr, status, thrown) {
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
  },

  get:function(path, success, error) {
    this.http("GET", path, success, error)
  },

  put:function(path, success, error) {
    this.http("PUT", path, success, error)
  },

  http:function(type, path, success, error) {
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
      success: success,
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
    App.get("/session/signout", function(data) {
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
    App.get("/session/whoami", function(data) {
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
  refresh: function() {
    App.get("/broker", function(json) {
      App.broker.setProperties(json);
      if( App.VirtualHostController.get('selected') == null && json.virtual_hosts.length > 0 ) {
        App.VirtualHostController.set('selected', json.virtual_hosts[0]);
      }
    });
  }
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
      App.get("/broker/virtual-hosts/"+selected, function(host) {
        App.virtual_host.setProperties(host);
        if( host.store ) {
          App.get("/broker/virtual-hosts/"+selected+"/store", function(store) {
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
    App.get("/broker/virtual-hosts/"+virtual_host+"/"+kind+"?ps=10000&f="+fields.join("&f="), function(data) {
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
    var self = this;
    App.put("/broker/virtual-hosts/"+virtual_host+"/"+kind+"/"+create_name, function(data) {
      self.refresh();
    });
  },

});

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
