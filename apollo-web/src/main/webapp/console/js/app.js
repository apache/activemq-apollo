App = Em.Application.create({
  window_size:{
    height:$(window).height(),
    width:$(window).width(),
  },

  refresh_interval:2,

  ready: function() {
    this.schedule_refresh();
    this._super();
    App.LoginController.refresh();
  },

  schedule_refresh: function() {
    var refresh_interval = App.get("refresh_interval");
    refresh_interval = Math.min(Math.max(1,refresh_interval), 360);
    setTimeout(function() {
      if( App.LoginController.get('is_logged_in') ) {
        App.auto_refresh();
      }
      App.schedule_refresh();
    }, refresh_interval*1000);
  },

  auto_refresh: function() {
    App.LoginController.auto_refresh();
    App.BrokerController.auto_refresh();
    App.VirtualHostController.auto_refresh();
    App.ConnectorController.auto_refresh();
    App.ConfigurationController.auto_refresh();
  },

  refresh: function() {
    App.BrokerController.refresh();
    App.VirtualHostController.refresh();
    App.ConnectorController.refresh();
    App.ConfigurationController.refresh();
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
      xhrFields: {
        withCredentials: true
      },
      dataType: 'json',
      success: function(data, textStatus, jqXHR){
        App.BrokerController.set("offline", false);
        if( success ) {
          success(data, textStatus, jqXHR)
        }
      },
      error: function(xhr, status, thrown) {
        if( xhr.status == 0 ) {
          App.BrokerController.set("offline", true);
        } else {
          App.BrokerController.set("offline", false);
          error(xhr, status, thrown)
        }
      },
    });
  },

});

function date_to_string(v) {
  var d = new Date(v);
  return d.toLocaleDateString()+" "+d.toLocaleTimeString();
}

App.PagedArrayController = Ember. ArrayController.extend({
  page:1,
  total_pages:1,

  is_on_first: function() {
    return this.get("page") == 1;
  }.property("page"),

  is_on_last: function() {
    var page = this.get("page");
    var total_pages = this.get("total_pages");
    return page == total_pages;
  }.property("page", "total_pages"),

  first:function() {
    this.set("page", 1)
  },
  last:function() {
    this.set("page", this.get("total_pages"))
  },
  next:function() {
    var total_pages = this.get("total_pages");
    var page = this.get("page");
    this.set("page", Math.min(total_pages, page+1))
  },
  prev:function() {
    var page = this.get("page");
    this.set("page", Math.max(1, page-1))
  },
});

App.MainController = Em.Controller.create({
  tabs: function() {
    var files = App.ConfigurationController.get("files")
    var rc = [];
    rc.push("Virtual Hosts")
    rc.push("Connectors")
    rc.push("Operating Environment")
    if( files && files.length > 0 ) {
      rc.push("Configuration")
    }
    return rc;
  }.property("App.ConfigurationController.files"),
  selected_tab:"Virtual Hosts",
  is_virtual_hosts_selected:function() {
    return this.get("selected_tab") == "Virtual Hosts"
  }.property("selected_tab"),
  is_connectors_selected:function() {
    return this.get("selected_tab") == "Connectors"
  }.property("selected_tab"),
  is_operating_environment_selected:function() {
    return this.get("selected_tab") == "Operating Environment"
  }.property("selected_tab"),
  is_configuration_selected:function() {
    return this.get("selected_tab") == "Configuration"
  }.property("selected_tab"),
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
      xhrFields: {
        withCredentials: true
      },
      data:{
        username: username,
        password: password
      },
      dataType: 'json',
      success: function(data){
        if( data ) {
          self.refresh();
          App.auto_refresh();
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


  auto_refresh:function() {
    if( !this.get('is_logged_in') ) {
      this.refresh();
    }
  },
  refresh: function(clear) {
    var was_logged_in = this.get('is_logged_in')
    var kind = this.get('kind')
    App.ajax("GET", "/session/whoami", function(data) {
        if( data.length==0 ) {
          App.ajax("GET", "/broker", function(broker) {
            data.push({kind:"UserPrincipal", name:"<anonymous>"});
            App.LoginController.set('content', data);
            if( App.LoginController.get('is_logged_in') ) {
              App.refresh();
            }
          }, function(error){
            App.LoginController.set('content', data);
            if( App.LoginController.get('is_logged_in') ) {
              App.refresh();
            }
          });
        } else {
          App.LoginController.set('content', data);
          if( App.LoginController.get('is_logged_in') ) {
            App.refresh();
          }
        }
      },
      function(xhr, status, thrown) {
        App.LoginController.set('content', null);
      });
  }
})

App.broker = Ember.Object.create({});
App.BrokerController = Ember.Controller.create({
  offline:false,
  auto_refresh: function() {
    if( App.broker.get("virtual_hosts") == null || App.MainController.get("is_operating_environment_selected")) {
      App.BrokerController.refresh();
    }
  },

  refresh: function() {
    App.ajax("GET", "/broker", function(json) {
      json.jvm_metrics.os_cpu_time = (json.jvm_metrics.os_cpu_time / 1000000000).toFixed(3) + " seconds"
      json.jvm_metrics.uptime = (json.jvm_metrics.uptime / 1000).toFixed(2) + " seconds"
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
  auto_refresh: function() {
    if( App.MainController.get("is_connectors_selected")) {
      App.ConnectorController.refresh();
    }
  },
  refresh: function() {
    var selected = this.get("selected")
    if( selected ) {
      App.ajax("GET", "/broker/connectors/"+selected, function(connector) {
        connector.state_date = date_to_string(connector.state_since);
        connector.paused = connector.state != "STARTED";
        if( connector.paused ) {
          connector.state_label = "Stopped";
        } else {
          connector.state_label = "Started";
        }
        App.connector.setProperties(connector);
      });
    }
    App.ConnectionsController.refresh();
  }.observes("selected"),

  start: function() {
    var selected = this.get("selected")
    App.ajax("POST", "/broker/connectors/"+selected+"/action/start", function(connector) {
      App.ConnectorController.refresh();
    });
  },

  stop: function() {
    var selected = this.get("selected")
    App.ajax("POST", "/broker/connectors/"+selected+"/action/stop", function(connector) {
      App.ConnectorController.refresh();
    });
  },

});


App.ConnectionsController = App.PagedArrayController.create({
  connectorBinding: "App.ConnectorController.selected",
  content:[],

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
    var page = this.get('page')
    var self = this;
    App.ajax("GET", "/broker/connections?p="+(page-1)+"&ps=25&o=id&q=connector='"+connector+"'&f="+fields.join("&f="), function(data) {
      updateArrayController(self, data.rows, function(item){ return item[0]; });
      self.set("page", data.page+1);
      self.set("total_pages", data.total_pages);
    });
  }.observes("connector", "page"),

  all_checked:false,
  check_all_toggle: function() {
    var all_checked= this.get("all_checked");
    this.get('content').forEach(function(item){
      item.set('checked', all_checked);
    });
  }.observes("all_checked"),
  has_checked: function() {
    return this.get('content').find(function(item){
      return item.get('checked')==true;
    }) != null;
  }.property("content.@each.checked"),

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
  style:function(){
    // Hide the virtual host details while the destination is displayed.
    var destination = App.get("destination");
    if( destination ) {
      return "display:none";
    } else {
      return null;
    }
  }.property("App.destination"),

  toggle_show_store: function(event) {
    this.set("show_store", !this.get("show_store"));
  },

  auto_refresh: function() {
    if( App.MainController.get("is_virtual_hosts_selected")) {
      var dest = App.get("destination");
      if( dest == null ) {
        App.VirtualHostController.refresh();
        App.DestinationsController.refresh();
      } else {
        App.DestinationController.refresh();
      }
    }
  },

  refresh: function() {
    var selected = this.get("selected")
    var self = this;
    if( selected ) {
      App.ajax("GET", "/broker/virtual-hosts/"+selected, function(host) {
        host.state_date = date_to_string(host.state_since);
        App.virtual_host.setProperties(host);
        if( host.store ) {
          if( self.get("show_store") ) {
            App.ajax("GET", "/broker/virtual-hosts/"+selected+"/store", function(store) {
              if( App.virtual_host_store.get("kind") == store.kind) {
                App.virtual_host_store.setProperties(store);
              } else {
                App.set("virtual_host_store", Ember.Object.create(store));
              }
            });
          }
        }
      });
    }

  },

  onSelectedChange: function() {
    App.set("destination", null)
    App.MessagesController.clear();
    this.refresh();
  }.observes("selected"),

  store_is_leveldb: function(){
   var clazz = App.get("virtual_host_store.@class")
   return clazz == "leveldb_store_status";
  }.property("App.virtual_host_store.@class"),

  store_is_bdb: function(){
   var clazz = App.get("virtual_host_store.@class")
   return clazz == "bdb_store_status";
  }.property("App.virtual_host_store.@class"),
});

App.DestinationsController = App.PagedArrayController.create({
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
    var page = this.get('page')
    var fields = ['id', 'metrics.queue_items', 'metrics.queue_size', 'metrics.producer_count', 'metrics.consumer_count'];
    var self = this;
    App.ajax("GET", "/broker/virtual-hosts/"+virtual_host+"/"+kind+"?p="+(page-1)+"&ps=25&o=id&f="+fields.join("&f="), function(data) {
      updateArrayController(self, data.rows, function(item){ return item[0]; });
      self.set("page", data.page+1);
      self.set("total_pages", data.total_pages);
    });
  }.observes("virtual_host", "kind", "page"),

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
  has_checked: function() {
    return this.get('content').find(function(item){
      return item.get('checked')==true;
    }) != null;
  }.property("content.@each.checked"),

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

  tabs:["Messages","Producers","Consumers"],
  selected_tab:"Messages",
  browse_count:0,

  destinationBinding:"App.destination",
  selectedBinding:"App.DestinationsController.selected",
  clear: function() {
    this.set('selected', null);
  },
  refresh: function() {
    var selected = this.get("selected")
    if( selected==null ) {
      App.set('destination', null);
      App.MessagesController.clear();
    } else {
      var virtual_host = App.DestinationsController.get("virtual_host");
      var kind = App.DestinationsController.get("kind");
      App.ajax("GET", "/broker/virtual-hosts/"+virtual_host+"/"+kind+"/"+selected+"?consumers=true&producers=true", function(data) {

        if( kind == "topics ") {
          App.set('browse_count', data.retained);
        } else {
          App.set('browse_count', data.metrics.queue_items);
        }
        data.state_date = date_to_string(data.state_since);
        data.metrics.enqueue_date = date_to_string(data.metrics.enqueue_ts);
        data.metrics.dequeue_date = date_to_string(data.metrics.dequeue_ts);
        data.metrics.nack_date = date_to_string(data.metrics.nack_ts);
        data.metrics.expired_date = date_to_string(data.metrics.expired_ts);
        data.producers.forEach(function(value){
          value.enqueue_date = date_to_string(value.enqueue_ts);
        });
        data.consumers.forEach(function(value){
          value.enqueue_date = date_to_string(value.enqueue_ts);
          if( value.ack_item_rate ) {
            value.ack_item_rate = value.ack_item_rate.toFixed(2);
          }
        });
        App.set('destination', data);
        App.MessagesController.auto_refresh();
      });
    }
  }.observes("selected"),
});

function updateArrayController(controller, data, keyFn) {
  if( data.length == 0 ) {
    controller.set("content", []);
  } else {
    var content = controller.get("content");
    var matches=true;

    var keyIndex = {}
    if( data.length == content.length ) {
      content.forEach(function(item, i){
        var key = keyFn(item);
        keyIndex[key] = item;
        if( key != keyFn(data[i]) ) {
          matches = false;
        }
      })
    } else {
      matches = false;
    }

    if( matches ) {
      content.forEach(function(item, index){
        item.setProperties(data[index]);
      });
    } else {

      var new_content = [];
      data.forEach(function(item){
        var obj = keyIndex[keyFn(item)];
        if( obj ) {
          obj.setProperties(item);
        } else {
          if( Object.prototype.toString.call(item) == '[object Array]' ) {
            obj = item;
          } else {
            obj = Ember.Object.create(item);
          }
        }
        new_content.push(obj);
      });
      controller.set("content", new_content);
    }
  }
}

App.MessagesController = Ember. ArrayController.create({
  content: [],

  toggle_headers: function(event) {
    event.context.set("show_headers", !event.context.get("show_headers"));
  },
  toggle_details: function(event) {
    event.context.set("show_details", !event.context.get("show_details"));
  },

  body: "",

  send: function() {
//    var virtual_host = this.get('virtual_host');
//    var kind = this.get('kind');
//    var create_name = this.get('create_name');
    this.set('body', "");
//    App.ajax("PUT", "/broker/virtual-hosts/"+virtual_host+"/"+kind+"/"+create_name, function(data) {
//      App.DestinationsController.refresh();
//    });
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

  max: 25,
  from: 0,

  reset:function() {
    this.set("from", 0)
    this.refresh();
  },

  auto_refresh:function() {
    var virtual_host = App.DestinationsController.get("virtual_host");
    var kind = App.DestinationsController.get("kind");
    var content = this.get("content");
    var selected = App.DestinationController.get("selected")
    var destination_path = "/broker/virtual-hosts/"+virtual_host+"/"+kind+"/"+selected;
    if( destination_path != this.get("destination_path") || content.get("length")==0 ) {
      this.refresh();
    }
  },

  refresh:function() {
    var virtual_host = App.DestinationsController.get("virtual_host");
    var kind = App.DestinationsController.get("kind");
    var selected = App.DestinationController.get("selected")
    var max_body = 100;
    var from = this.get("from");
    var max = this.get("max");
    var destination_path = "/broker/virtual-hosts/"+virtual_host+"/"+kind+"/"+selected;
    App.ajax("GET", destination_path+"/messages?from="+from+"&max="+max+"&max_body="+max_body, function(page) {
      var data = page.messages;
      data.forEach(function(item, index){
        if( item.base64_body ) {
          var rc = atob(item.base64_body);
          if( item.body_truncated ) {
            rc += "..."
          }
          item.body = rc;
        }
        if( item.expiration==0 ) {
          item.expiration = "no"
        } else {
          item.expiration = date_to_string(item.expiration);
        }
      });
      if( data.length == 0 ) {
        App.MessagesController.set("from", 0);
      } else {
        App.MessagesController.set("from", data[0].entry.seq);
      }
      updateArrayController(App.MessagesController, data, function(item){ return item.entry.seq; });
      App.MessagesController.set("destination_path", destination_path);
    });
  },

  prev: function() {
    var content = this.get("content");
    if( content.length > 0 ) {
      var seq = content[0].get("entry.seq")
      this.set("from", seq-this.get("max"));
      this.refresh();
    }
  },

  next: function() {
    var content = this.get("content");
    if( content.length > 0 ) {
      var seq = content[content.length-1].get("entry.seq")
      this.set("from", seq+1);
      this.refresh();
    }
  },

});

App.ConfigurationController = Ember.Controller.create({

  offline:false,
  files:null,
  selected:"apollo.xml",
  actual_selected:"apollo.xml",
  buffer:null,
  original:null,
  buffers:{},

  selected_ace_mode: function(){
    var selected = this.get("actual_selected");
    var re = /(?:\.([^.]+))?$/;
    var mode = re.exec(selected)[1] || "text"
    if( mode == "pem" || mode == "p12" || mode == "properties" ||
        mode=="config" || mode == "keystore" || mode=="txt" ) {
      mode = "text";
    }
    return mode;
  }.property("actual_selected"),

  modified: function() {
    return this.get("original") != this.get("buffer");
  }.property("original", "buffer"),

  is_modified: function() {
    return this.get("original") != this.get("buffer");
  },

  auto_refresh: function() {
    if( App.MainController.get("is_configuration_selected")) {
      this.refresh();
      if(App.ConfigurationController.get("buffer")==null ) {
        this.refresh_selected();
      }
    }
  },

  refresh: function() {
    App.ajax("GET", "/broker/config/files", function(json) {
      App.ConfigurationController.set("files", json);
    },
    function(xhr, status, thrown) {
      if( xhr.status == 401 || xhr.status == 404 ) {
        App.ConfigurationController.set("files", null);
      } else {
        App.default_error_handler(xhr, status, thrown)
      }
   });
  },

  refresh_selected: function() {
    var self = this;
    var selected = this.get("actual_selected");
    this.ajax("GET", "/broker/config/files/"+selected, function(buffer) {
      self.set("original", buffer);
      self.set("buffer", buffer);
    }, function(error, a, b, c){
      self.set("original", null);
      self.set("buffer", null);
    });
  },

  ajax:function(type, path, success, error, data) {
    $.ajax({
      type: type,
      url: "../api/json"+path,
      headers: {
        AuthPrompt:'false',
      },
      xhrFields: {
        withCredentials: true
      },
      headers: {
        "Accept":"application/octet-stream",
      },
      dataType: 'text',
      success: function(data, textStatus, jqXHR){
        App.BrokerController.set("offline", false);
        if( success ) {
          success(data, textStatus, jqXHR)
        }
      },
      error: error,
      data:data,
      processData:false,
      contentType:"application/octet-stream",
    });
  },

  on_selected_change:function() {
    var self = this;
    var actual_selected = this.get("actual_selected");
    var selected = this.get("selected");
    if( actual_selected == selected ) {
      return;
    }
    if( !this.is_modified() ) {
      this.set("actual_selected", selected);
    } else {
      Bootstrap.ModalPane.popup({
        heading: "Are you sure?",
        message: "Switching files will cause you to loose the current edits you have made to the current file.",
        primary: "OK",
        secondary: "Cancel",
        showBackdrop: true,
        callback: function(opts, event) {
          if (opts.primary) {
            self.set("actual_selected", selected);
          } else {
            self.set("selected", actual_selected);
          }
        }
      });
    }
  }.observes("selected"),

  on_actual_selected_change:function() {
    this.refresh_selected();
  }.observes("actual_selected"),

  save: function() {
    var self = this;
    var selected = this.get("actual_selected");
    this.ajax("POST", "/broker/config/files/"+selected, function(buffer) {
      self.set("original", buffer);
      Bootstrap.AlertMessage.create({
        type:"info",
        message:"File "+selected+" saved."
      }).appendTo("#notifications")
    }, function(error, a, b, c){
      Bootstrap.AlertMessage.create({
        type:"warning",
        message:"File "+selected+" could not be saved: "+a
      }).appendTo("#notifications")
    }, this.get("buffer"));
  },

});

App.NumberField = Ember.TextField.extend({
    _validation: function() {
      var value = this.get('value');
      value = value.toString().replace(/[^\d.]/g, "");
      this.set('value', value);
    }.observes('value')
});

App.Tooltip = Ember.View.extend({
  tagName:'a',
  attributeBindings: ['href', 'title'],
  href:'#',
  title:'Example',
  didInsertElement: function() {
    this.$().tooltip();
  },
  click: function(event){
    this.$().tooltip('toggle')
  },
  mouseEnter: function(event){
    this.$().tooltip('show')
  },
  mouseLeave: function(event){
    this.$().tooltip('hide')
  },
});

App.AceView = Ember.View.extend({

  editor: null,
  mode:"xml",
  onModeChange:function(){
    var editor = this.get("editor")
    var mode = this.get("mode")
    if( editor!=null ) {
      editor.getSession().setMode("ace/mode/"+mode);
    }
  }.observes('editor', 'mode'),

  theme:"monokai",
  onThemeChange:function(){
    var editor = this.get("editor")
    var theme = "ace/theme/"+this.get("theme")
    if( editor!=null ) {
      editor.setTheme(theme);
    }
  }.observes('editor', 'theme'),

  didInsertElement: function() {
    var self = this;
    var id = this.$().attr('id');
    var editor = ace.edit(id);

    editor.renderer.setShowPrintMargin(true);
    editor.renderer.setShowGutter(true);
    editor.setHighlightActiveLine(false);
    var session = editor.getSession();
    session.setUseSoftTabs(true);
    session.setTabSize(2);
    session.on('change', function (e) {
      self.onEditorTextChange(session, e);
    });
    this.set('editor', editor);
  },

  onStyleChange: function() {
    var editor = this.get('editor');
    if( editor!=null ) {
      var div = this.$()
      div.css("height", $(window).height()-(div.offset().top+40));
      editor.resize();
    }
  }.observes('editor', 'App.window_size'),

  onViewChange: function() {
    if( this._updating )
      return;
    var editor = this.get('editor');
    var code = this.get('code');
    this._updating = true
    if (editor && code && code !== this._codeFromEditor) {
      this._codeFromUpdate = code;
      editor.setValue(code);
      editor.clearSelection();
      editor.focus();
    }
    this._updating = false
  }.observes('code', 'editor'),

  onEditorTextChange: function(session, e) {
    if( this._updating )
      return;
    var self = this;
    var code = session.getValue();
    if (code !== self._codeFromUpdate) {
      this._updating = true;
      this._codeFromEditor = code;
        console.log("Setting: ["+code+"]");
        self.set('code', code);
        console.log("Done");
      this._updating = false;
    }
  }
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

App.SettingsView = Ember.View.create({
  templateName: 'settings',
  classNames:["modal", "hide", "fade"],
  show: function(){
    var id = this.$().attr('id');
    $("#"+id).modal("show");
  },
  hide: function(){
    var id = this.$().attr('id');
    $("#"+id).modal("hide");
  }
})
App.SettingsView.appendTo("#content-holder")


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


// Install a base64 decoding function if one is not available.
if( !window.atob ) {
  window.atob = function(text){
    text = text.replace(/\s/g,"");
    if(!(/^[a-z0-9\+\/\s]+\={0,2}$/i.test(text)) || text.length % 4 > 0){
      throw new Error("Not a base64-encoded string.");
    }
    var digits = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
      cur, prev, digitNum,
      i=0,
      result = [];
    text = text.replace(/=/g, "");
    while(i < text.length){
      cur = digits.indexOf(text.charAt(i));
      digitNum = i % 4;
      switch(digitNum){
        case 1:
          result.push(String.fromCharCode(prev << 2 | cur >> 4));
          break;
        case 2:
          result.push(String.fromCharCode((prev & 0x0f) << 4 | cur >> 2));
          break;
        case 3:
          result.push(String.fromCharCode((prev & 3) << 6 | cur));
          break;
      }
      prev = cur;
      i++;
    }
    return result.join("");
  }
}

Ember.Handlebars.registerHelper("key_value", function(name, fn) {
  var obj = this[name];
  var buffer = "", key;
  for (key in obj) {
    if (obj.hasOwnProperty(key)) {
      buffer += fn({key: key, value: obj[key]});
    }
  }
  return buffer;
});

Ember.Handlebars.registerHelper('memory', function(property, options) {
  options.fn = function(size) {
    if( (typeof size)=="number" ) {
      var units = "bytes"
      if( size > 1024 ) {
        size = size / 1024;
        units = "kb"
        if( size > 1024 ) {
          size = size / 1024;
          units = "mb"
          if( size > 1024 ) {
            size = size / 1024;
            units = "gb"
            if( size > 1024 ) {
              size = size / 1024;
              units = "tb"
            }
          }
        }
        size = size.toFixed(2);
      } else {
        if( (""+size).indexOf(".") !== -1 ) {
          size = size.toFixed(2);
        }
      }
      return size+" "+units;
    }
    return size;
  }
  return Ember.Handlebars.helpers.bind(property, options);
});
Ember.Handlebars.registerHelper('hex', function(property, options) {
  options.fn = function(size) {
    if( (typeof size)=="number" ) {
      return "0x"+size.toString(16);
    }
    return size;
  }
  return Ember.Handlebars.helpers.bind(property, options);
});

$(window).resize(function() {
 App.set("window_size", {
  height:$(window).height(),
  width:$(window).width(),
 })
});

App.initialize();
