package org.apache.activemq.apollo.broker.store

import java.util.concurrent.atomic.AtomicLong
import org.fusesource.hawtbuf.{DataByteArrayInputStream, AbstractVarIntSupport, DataByteArrayOutputStream, Buffer}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class PersistentLongCounter(name:String, increment:Long=1000) {

  def encode(a1:Long):Buffer = {
    val out = new DataByteArrayOutputStream(
      AbstractVarIntSupport.computeVarLongSize(a1)
    )
    out.writeVarLong(a1)
    out.toBuffer
  }

  def decode(bytes:Buffer):Long = {
    val in = new DataByteArrayInputStream(bytes)
    in.readVarLong()
  }

  @transient
  var store:Store = _
  val counter = new AtomicLong(0)
  val limit = new AtomicLong(0)

  val key = Buffer.utf8("long-counter:"+name);

  def init(store:Store)(on_complete: =>Unit):Unit = {
    connect(store)
    store.get(key) { value =>
      val c = value.map(decode(_)).getOrElse(0L)
      counter.set(c)
      limit.set(c+increment)
      update(c+increment)(on_complete)
    }
  }

  def disconnect(on_complete: =>Unit):Unit = {
    update(get)(on_complete)
    this.store = null
  }

  def connect(store:Store) = {
    this.store = store
  }

  def get = counter.get

  def incrementAndGet() = {
    val rc = counter.incrementAndGet()
    var done = false
    while( !done ) {
      val l = limit.get
      if ( rc < l ) {
        done = true
      } else if ( limit.compareAndSet(l, l+increment) ) {
        update(l + increment)()
      }
    }
    rc
  }

  def update(value: Long)(on_complete: =>Unit) {
    val s = store
    if (s!=null) {
      val uow = s.create_uow()
      uow.put(key, encode(value))
      uow.complete_asap()
      uow.on_complete(on_complete)
      uow.release()
    }
  }

}