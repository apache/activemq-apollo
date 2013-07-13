/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.apollo.broker.security

import java.lang.Boolean
import org.apache.activemq.apollo.dto.AccessRuleDTO
import java.util.regex.Pattern
import java.util.concurrent.atomic.AtomicLong
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.util.Log

object SecuredResource {
  case class SecurityRules(version:Long, rules: Seq[(String,SecurityContext)=>Option[Boolean]])

  val ADMIN   = "admin"
  val MONITOR = "monitor"
  val CONFIG  = "config"
  val CONNECT = "connect"
  val CREATE  = "create"
  val DESTROY = "destroy"
  val SEND    = "send"
  val RECEIVE = "receive"
  val CONSUME = "consume"

  sealed trait ResourceKind {
    val id:String
    def actions:Set[String]
  }
  object BrokerKind extends ResourceKind {
    val id = "broker"
    val actions = Set(ADMIN, MONITOR, CONFIG)
  }
  object VirtualHostKind extends ResourceKind{
    val id = "virtual-host"
    val actions = Set(ADMIN, MONITOR, CONFIG, CONNECT)
  }
  object ConnectorKind extends ResourceKind{
    val id = "connector"
    val actions = Set(ADMIN, MONITOR, CONFIG, CONNECT)
  }
  object TopicKind extends ResourceKind{
    val id = "topic"
    val actions = Set(ADMIN, MONITOR, CONFIG, CREATE, DESTROY, SEND, RECEIVE)
  }
  object TopicQueueKind extends ResourceKind{
    val id = "topic-queue"
    val actions = Set(ADMIN, MONITOR, CONFIG, CREATE, DESTROY, SEND, RECEIVE)
  }
  object QueueKind extends ResourceKind{
    val id = "queue"
    val actions = Set(ADMIN, MONITOR, CONFIG, CREATE, DESTROY, SEND, RECEIVE, CONSUME)
  }
  object DurableSubKind extends ResourceKind{
    val id = "dsub"
    val actions = Set(ADMIN, MONITOR, CONFIG, CREATE, DESTROY, SEND, RECEIVE, CONSUME)
  }
  object OtherKind extends ResourceKind{
    val id = "other"
    val actions = Set[String]()
  }
}
import SecuredResource._

trait SecuredResource {
  def resource_kind:ResourceKind
  def id:String

  @volatile
  var rules_cache:SecurityRules = _
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Authorizer {
  def can(ctx:SecurityContext, action:String, resource:SecuredResource):Boolean;
}

object Authorizer {

  val version_counter = new AtomicLong()

  def apply():Authorizer = new Authorizer() {
    def can(ctx: SecurityContext, action: String, resource: SecuredResource) = true
  }

  def apply(broker:Broker):Authorizer = {
    import collection.JavaConversions._
    val pk = broker.authenticator.acl_principal_kinds
    val rules = broker.config.access_rules.toList.flatMap(parse(broker.console_log, _,pk))
    new RulesAuthorizer(version_counter.incrementAndGet(), rules.toArray )
  }

  def apply(host:VirtualHost=null):Authorizer = {
    import collection.JavaConversions._
    val pk = host.authenticator.acl_principal_kinds
    val rules =  host.config.access_rules.toList.flatMap(parse(host.console_log, _,pk, host)) :::
        host.broker.config.access_rules.toList.flatMap(parse(host.broker.console_log, _,pk))
    new RulesAuthorizer(version_counter.incrementAndGet(), rules.toArray )
  }

  def parse(log:Log, rule:AccessRuleDTO, default_principal_kinds:Set[String], host:VirtualHost=null):Option[ResourceMatcher] ={
    import log._
    var resource_matchers = List[(SecuredResource)=>Boolean]()

    val actions = Option(rule.action).map(_.trim().toLowerCase).getOrElse("*") match {
      case "*" => null
      case action =>
        val rc = action.split("\\s").map(_.trim()).toSet
        // Not all actions can apply to all resource types.
        resource_matchers ::= ((resource:SecuredResource) => {
          !(resource.resource_kind.actions & rc).isEmpty
        })
        rc
    }

    for(id_regex <- Option(rule.id_regex)) {
      val reg_ex = Pattern.compile(id_regex)
      resource_matchers ::= ((resource:SecuredResource) => {
        reg_ex.matcher(resource.id).matches()
      })
    }

    Option(rule.id).getOrElse("*") match {
      case "*" =>
      case id =>
        if(rule.kind == QueueKind.id || rule.kind == TopicKind.id ) {
          val filter = LocalRouter.destination_parser.decode_filter(id)
          resource_matchers ::= ((resource:SecuredResource) => {
            filter.matches(LocalRouter.destination_parser.decode_path(resource.id))
          })
        } else {
          resource_matchers ::= ((resource:SecuredResource) => {
            resource.id == id
          })
        }
    }

    Option(rule.kind).map(_.trim().toLowerCase).getOrElse("*") match {
      case "*" =>
        if( host!=null) {
          resource_matchers ::= ((resource:SecuredResource) => {
            resource.resource_kind match {
              case BrokerKind=> false
              case ConnectorKind=> false
              case _ => true
            }
          })
        }
      case kind =>
        val kinds = (kind.split("\\s").map(_.trim()).map{ v=>
          val kind:ResourceKind = v match {
            case BrokerKind.id =>
              if(host!=null) {
                warn("Ignoring invalid access rule. kind='broker' can only be configured at the broker level: "+rule)
                return None
              }
              BrokerKind
            case ConnectorKind.id =>
              if(host!=null) {
                warn("Ignoring invalid access rule. kind='connector' can only be configured at the broker level: "+rule)
                return None
              }
              ConnectorKind
            case VirtualHostKind.id =>VirtualHostKind
            case QueueKind.id =>QueueKind
            case TopicKind.id =>TopicKind
            case DurableSubKind.id =>DurableSubKind
            case _ =>
              warn("Ignoring invalid access rule. Unknown kind '"+v+"' "+rule)
              return None
          }
          kind
        }).toSet
        resource_matchers ::= ((resource:SecuredResource) => {
          kinds.contains(resource.resource_kind)
        })
    }

    val principal_kinds = Option(rule.principal_kind).map(_.trim()).getOrElse(null) match {
      case null => Some(default_principal_kinds)
      case "*" => None
      case principal_kind => Some(principal_kind.split("\\s").map(_.trim()).toSet)
    }

    def connector_match(func:(SecurityContext)=>Boolean):(SecurityContext)=>Boolean = {
      Option(rule.connector).getOrElse("*") match {
        case "*" => func
        case connector_id =>
          (ctx:SecurityContext) => {
            ctx.connector_id==connector_id && func(ctx)
          }
      }
    }
    
    def parse_principals(value:String): Option[(SecurityContext)=>Boolean] = {
      Option(value).map(_.trim() match {
        case "*" =>
          connector_match((ctx:SecurityContext) => { true })
        case "+" =>
          // user has to have at least one of the principle kinds
          connector_match((ctx:SecurityContext) => {
            principal_kinds match {
              case Some(principal_kinds)=>
                ctx.principals.find(p=> principal_kinds.contains(p.getClass.getName) ).isDefined
              case None =>
                !ctx.principals.isEmpty
            }
          })
        case principal =>
          val principals = if(rule.separator!=null) {
            principal.split(Pattern.quote(rule.separator)).map(_.trim()).toSet
          } else {
            Set(principal)
          }
          connector_match((ctx:SecurityContext) => {
            principal_kinds match {
              case Some(principal_kinds)=>
                ctx.principals.find{ p=>
                  val km = principal_kinds.contains(p.getClass.getName)
                  val nm = principals.contains(p.getName)
                  km && nm
                }.isDefined
              case None =>
                ctx.principals.find(p=> principals.contains(p.getName) ).isDefined
            }
          })
      })
    }

    val allow = parse_principals(rule.allow)
    val deny = parse_principals(rule.deny)

    if( allow.isEmpty && deny.isEmpty ) {
      warn("Ignoring invalid access rule. Either the 'allow' or 'deny' attribute must be declared.")
      return None
    }

    Some(ResourceMatcher(resource_matchers, actions, allow, deny))
  }

  case class ResourceMatcher(
    resource_matchers:List[(SecuredResource)=>Boolean],
    actions:Set[String],
    allow:Option[(SecurityContext)=>Boolean],
    deny:Option[(SecurityContext)=>Boolean]
  ) {

    def resource_matches(resource:SecuredResource):Boolean = {
      // Looking for a matcher that does not match so we can
      // fail the match quickly.
      !resource_matchers.find(_(resource)==false).isDefined
    }

    def action_matches(action:String, ctx:SecurityContext):Option[Boolean] = {
      if(actions!=null && !actions.contains(action)) {
        return None
      }
      for(matcher <- deny) {
        if ( matcher(ctx) ) {
          return Some(false)
        }
      }
      for(matcher <- allow) {
        if ( matcher(ctx) ) {
          return Some(true)
        }
      }
      return None
    }
  }

  case class RulesAuthorizer(version:Long, config:Array[ResourceMatcher]) extends Authorizer {

    def can(ctx:SecurityContext, action:String, resource:SecuredResource):Boolean = {
      if (ctx==null) {
        return true;
      }

      // we may need to rebuild the security rules cache.
      var cache = resource.rules_cache
      if( cache==null || cache.version != version ) {
        // We cache the list of rules which match this resource so that
        // future access checks don't have to process the whole list.
        cache = SecurityRules(version, build_rules(resource))
        resource.rules_cache = cache
      }

      // Now we process the rules that are specific to the resource.
      for( rule <- cache.rules ) {
        rule(action, ctx) match {
          case Some(allow)=>
            // First rule match controls if we allow or reject access.
            return allow;
          case None=>
        }
      }

      // If no rules matched, then reject access.
      return false;
    }
    def build_rules(resource:SecuredResource):Seq[(String,SecurityContext)=>Option[Boolean]] = {
      config.flatMap{rule=>
        if(rule.resource_matches(resource))
          Some(rule.action_matches _)
        else
          None
      }
    }
  }
}

