package org.apache.activemq.apollo.broker.security

import java.lang.Boolean
import collection.mutable.{ListBuffer, HashMap}
import org.apache.activemq.apollo.broker.LocalRouter
import org.apache.activemq.apollo.dto.{QueueDTO, AccessRuleDTO}
import org.apache.activemq.apollo.util.path.PathParser._
import java.util.regex.Pattern
import org.apache.activemq.apollo.broker.security.Authorizer.ResourceMatcher
import java.util.concurrent.atomic.AtomicLong

object SecuredResource {
  case class SecurityRules(version:Long, rules: Seq[(String,SecurityContext)=>Option[Boolean]])

  sealed trait ResourceKind
  object BrokerKind extends ResourceKind
  object VirtualHostKind extends ResourceKind
  object ConnectorKind extends ResourceKind
  object QueueKind extends ResourceKind
  object TopicKind extends ResourceKind
  object DurableSubKind extends ResourceKind
  object OtherKind extends ResourceKind
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

  def apply(config:Seq[AccessRuleDTO], default_principal_kinds:Set[String]):Authorizer = {
    new RulesAuthorizer(version_counter.incrementAndGet(), config.map(ResourceMatcher(_, default_principal_kinds)))
  }

  case class ResourceMatcher(rule:AccessRuleDTO, default_principal_kinds:Set[String]) {

    var resource_matchers = List[(SecuredResource)=>Boolean]()

    for(id_regex <- Option(rule.id_regex)) {
      val reg_ex = Pattern.compile(id_regex)
      resource_matchers ::= ((resource:SecuredResource) => {
        reg_ex.matcher(resource.id).matches()
      })
    }

    Option(rule.id).getOrElse("*") match {
      case "*" =>
      case id =>
        if(rule.kind == "queue" || rule.kind == "topic") {
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
      case kind =>
        val kinds = (kind.split(",").map(_.trim()).map{ v=>
          val kind:ResourceKind = v match {
            case "broker"=>BrokerKind
            case "virtual-host"=>VirtualHostKind
            case "connector"=>ConnectorKind
            case "queue"=>QueueKind
            case "topic"=>TopicKind
            case "dsub"=>DurableSubKind
            case _ => OtherKind
          }
          kind
        }).toSet
        resource_matchers ::= ((resource:SecuredResource) => {
          kinds.contains(resource.resource_kind)
        })
    }

    def resource_matches(resource:SecuredResource):Boolean = {
      // Looking for a matcher that does not match so we can
      // fail the match quickly.
      !resource_matchers.find(_(resource)==false).isDefined
    }

    var action_matchers = List[(String, SecurityContext)=>Boolean]()

    val principal_kinds = Option(rule.principal_kind).map(_.trim().toLowerCase).getOrElse(null) match {
      case null => Some(default_principal_kinds)
      case "*" => None
      case principal_kind => Some(principal_kind.split(",").map(_.trim()).toSet)
    }

    Option(rule.principal).map(_.trim().toLowerCase).getOrElse("+") match {
      case "*" =>
      case "+" =>
        // user has to have at least one of the principle kinds
        action_matchers ::= ((action:String, ctx:SecurityContext) => {
          principal_kinds match {
            case Some(principal_kinds)=>
              ctx.principles.find(p=> principal_kinds.contains(p.getClass.getName) ).isDefined
            case None =>
              !ctx.principles.isEmpty
          }
        })

      case principal =>
        val principals = if(rule.separator!=null) {
          principal.split(Pattern.quote(rule.separator)).map(_.trim()).toSet
        } else {
          Set(principal)
        }
        action_matchers ::= ((action:String, ctx:SecurityContext) => {
          principal_kinds match {
            case Some(principal_kinds)=>
              ctx.principles.find{ p=>
                val km = principal_kinds.contains(p.getClass.getName)
                val nm = principals.contains(p.getName)
                km && nm
              }.isDefined
            case None =>
              ctx.principles.find(p=> principals.contains(p.getName) ).isDefined
          }
        })
    }

    Option(rule.action).map(_.trim().toLowerCase).getOrElse("*") match {
      case "*" =>
      case action =>
        val actions = Set(action.split(",").map(_.trim()): _* )
        action_matchers ::= ((action:String, ctx:SecurityContext) => {
          actions.contains(action)
        })
    }

    val deny = Option(rule.deny).map(_.booleanValue()).getOrElse(false)

    def action_matches(action:String, ctx:SecurityContext):Option[Boolean] = {
      for(matcher <- action_matchers) {
        if ( !matcher(action, ctx) ) {
          return None
        }
      }
      return Some(!deny)
    }
  }

  case class RulesAuthorizer(version:Long, config:Seq[ResourceMatcher]) extends Authorizer {

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

