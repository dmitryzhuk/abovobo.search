package org.abovobo.search

import scala.collection.mutable.HashMap
import akka.actor.Cancellable
import akka.actor.Scheduler
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext


class TransactionManager[ID, TData](
    scheduler: Scheduler,
    timeout: FiniteDuration, 
    timeoutHandler: (ID, TData) => Any = (a: ID, b: TData) => {})
    (implicit executor: ExecutionContext) {
	
  private class Transaction(val data: TData, val timer: Cancellable)
	
  private val transactions = new HashMap[ID, Transaction]
  
	def add(id: ID, data: TData) {
	  val prev = transactions.put(id, new Transaction(data, scheduler.scheduleOnce(timeout)(timeout(id))))
	  if (prev.isDefined) throw new IllegalStateException("duplicate transaction entry")
	  
    //	  // cancel previous transaction timer, actually 
    //	  prev foreach { _ => fail(id) }	  
    //	  prev.map(_.data)
	}
	
	def remove(id: ID): Option[TData] = {
	  val t  = transactions.remove(id)	  
	  t foreach { _.timer.cancel() }
	  t.map(_.data)
	} 
	
	def get(id: ID): Option[TData] = transactions.get(id).map(_.data)
		
	def complete(id: ID) = remove(id)

	def fail(id: ID) = remove(id) 
	
	def timeout(id: ID) = remove(id) match {
	  case Some(data) => timeoutHandler(id, data); Some(data)
	  case None => None
	}
	
	def list: scala.collection.Map[ID, TData] = transactions.mapValues(_.data)
}