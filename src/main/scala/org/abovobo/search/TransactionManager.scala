package org.abovobo.search

import scala.collection.mutable
import akka.actor.Cancellable
import akka.actor.Scheduler
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext


class TransactionManager[ID, TData](
    scheduler: Scheduler,
    timeout: FiniteDuration, 
    timeoutHandler: ID => Any)
    (implicit executor: ExecutionContext) {
	
  private class Transaction(val data: TData, val timer: Cancellable)
	
  private val transactions = new mutable.HashMap[ID, Transaction]
  
	def add(id: ID, data: TData) {
	  val prev = transactions.put(id, new Transaction(data, scheduler.scheduleOnce(timeout)(timeoutHandler(id))))
	  if (prev.isDefined) throw new IllegalStateException("duplicate transaction entry")
	}
	
	def remove(id: ID): Option[TData] = {
	  val t  = transactions.remove(id)	  
	  t foreach { _.timer.cancel() }
	  t.map(_.data)
	} 
	
	def get(id: ID): Option[TData] = transactions.get(id).map(_.data)
		
	def complete(id: ID) = remove(id)

	def fail(id: ID) = remove(id) 
		
	def list: scala.collection.Map[ID, TData] = transactions.mapValues(_.data)
}