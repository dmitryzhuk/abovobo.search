package org.abovobo.search

import akka.actor.Actor
import akka.actor.ActorLogging
import scala.concurrent.duration._
import org.abovobo.search.SearchPlugin._
import org.abovobo.dht.TID

class IndexManagerActor(indexManager: IndexManager) extends Actor with ActorLogging {
  import IndexManagerActor._
  val system = context.system
  import system.dispatcher
  
  override def postStop() {
    indexManager.close()
  }
  
  override def receive = {
    case IndexManagerCommand(id, cmd) => cmd match {
      case Announce(item, _) => indexManager.offer(item)
      case Lookup(searchString, _) =>
        // Note: there will be no SearchFinished message!
        //sender ! IndexManagerResponse(id, FoundItems(searchString, indexManager.search(searchString)))
    }
    case Cleanup =>
      indexManager.cleanup()
      context.system.scheduler.scheduleOnce(IndexManagerActor.CleanupInterval, self, Cleanup)

    case Clear => indexManager.clear()
  }
    
  if (indexManager.cleanupNeeded) {
    context.system.scheduler.scheduleOnce(30.seconds, self, Cleanup)
  }
}
object IndexManagerActor {
  case class IndexManagerCommand[T <: SearchPlugin.Command](id: TID, cmd: T)
  case class IndexManagerResponse[T <: SearchPluginActor.Response](id: TID, response: T)
  case object Clear
  case object Cleanup
   
  val CleanupInterval: FiniteDuration = 1.day
}
