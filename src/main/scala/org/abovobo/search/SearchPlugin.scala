package org.abovobo.search

import org.abovobo.dht.Plugin
import org.abovobo.dht.Controller
import org.abovobo.dht.persistence.Reader
import org.abovobo.dht.PluginMessage
import akka.actor.ActorLogging
import org.abovobo.search.ContentIndex.ContentItem
import scala.pickling.binary.BinaryPickle
import scala.pickling.binary.BinaryPickleFormat
import org.abovobo.integer.Integer160
import org.abovobo.search.ContentIndex.ContentRef
import org.abovobo.dht.Controller.SendPluginMessage
import java.net.InetSocketAddress
import org.abovobo.dht.PluginMessage
import org.abovobo.dht.TID
import org.abovobo.dht.Node
import akka.actor.ActorRef
import scala.concurrent.duration._
import org.abovobo.search.ContentIndex.CID
import scala.collection.mutable
import scala.collection.mutable.HashSet
import akka.actor.Cancellable
import org.abovobo.dht.TIDFactory
import akka.actor.Actor
import org.abovobo.search.SearchPlugin.IndexManagerActor._
import akka.actor.Props

class SearchPlugin(selfId: Integer160, pid: Plugin.PID, dhtController: ActorRef, indexManager: ActorRef, dhtTable: Reader) extends Plugin(pid) with ActorLogging {
  import scala.pickling._
  import scala.pickling.binary._
  import SearchPlugin._

  val system = this.context.system
  import system.dispatcher
  
  val announceTtl = 3
  val searchTtl = 3

  // XXX: to params?
  /// searchString -> SearchOperations
  val currentRequests: TransactionManager[TID, SearchOperation] = new TransactionManager(this.context.system.scheduler, 5.seconds, { (id) => self ! SearchTimeout(id) })
  val tidFactory = new TIDFactory

  override def receive = {
    case Controller.Received(message, remote) =>
      message match {
        case pluginMessage: PluginMessage => {
          implicit val pm = pluginMessage
          implicit val rm = remote
          
          pluginMessage.payloadBytes.unpickle[SearchMessage] match {
            case SearchNetworkCommand(cmd, ttl) => {
              this.log.debug("Got command: " + cmd)

              cmd match {
                case Announce(item) => announce(item, ttl)
                case Lookup(searchString) => {
                  val responder = new NetworkResponder(new Node(pluginMessage.id, remote), pluginMessage)
                  search(searchString, responder, ttl)
                }
              }
            }
            case _: Command => throw new IllegalStateException("command should be always wrapped into network command")
            case r: Response => {
              // TODO: end transaction 
              this.log.debug("Got response message: " + r)
              r match {
                case FoundItems(searchString, found) =>
                case SearchFinished(searchString) =>
                case Error(code, text) =>
              }
            }
          }
        } // case PluginMessage
        case _ => this.log.error("Wrong message type recieved from DHT Controller")
      }
            
    case Announce(item) => announce(item, announceTtl)
    
    case Lookup(searchString) => search(searchString, new DirectResponder(sender), searchTtl)
    
    case IndexManagerResponse(tid, response) => response match {
        case FoundItems(searchString, refs) => currentRequests.get(tid) match {
            case Some(search) => {
              log.debug("Local response: " + response)
              reportResults(search, selfId, refs)
            }
            case None => log.info("local response for unknown/expired request: " + tid + ", " + response)
        }
        case _ => throw new IllegalStateException
    }
      
    case SearchTimeout(tid) => currentRequests.fail(tid) match {
      case Some(search) => {
        log.debug("finishing search " + tid + " " + search.searchString + " by timeout. Not responded: " + search.pendingQueries.size)
        search.finish() 
      }
      case None => log.info("timeout for unknown/expired request: " + tid)
    }

    // XXX: 
    // here it should be like
    // case error: Error => 
    // from Controller. Or something like that
  }
  
  def announce(item: ContentItem, ttl: Int) {
    indexManager ! IndexManagerCommand(tidFactory.next, Announce(item))
    if (ttl > 1)  {
      announceToNetwork(item, ttl - 1)
    }
  }
  
  def announceToNetwork(item: ContentItem, ttl: Int) {
    // TODO: grab nodes, send announce
  }
  
  def search(searchString: String, responder: Responder, ttl: Int) {
    val tid = tidFactory.next
    val searchOp = new SearchOperation(tid, searchString, responder)

    currentRequests.add(tid, searchOp)
    
    indexManager ! IndexManagerCommand(tid, Lookup(searchString))
    
    if (ttl > 1) {
      searchInNetwork(searchOp, ttl - 1)
    }
  }
  
  def searchInNetwork(searchOp: SearchOperation, ttl: Int) {
    // todo: grab nodes, send lookup
  }
  
  def reportResults(search: SearchOperation, from: Integer160, items: Traversable[ContentRef]) = {
    val uniqueResults = items.filterNot { i=> search.reportedResults.contains(i.id) }

    if (!uniqueResults.isEmpty) {
      search.respond(FoundItems(search.searchString, uniqueResults))    
      search.reportedResults ++= uniqueResults.map(_.id)      
    }

    search.pendingQueries -= from
    
    if (search.pendingQueries.isEmpty) {
      // finish search, cleanup
      search.finish()
      currentRequests.complete(search.tid)
    } 
  }
  
  
  // private messages
  case class SearchNetworkCommand(cmd: Command, ttl: Byte) extends SearchMessage
  case class SearchTimeout(tid: TID)
  
  /// active searches state
  
  trait Responder extends (Response => Any) 

  class DirectResponder(sender: ActorRef) extends Responder {
    def apply(response: Response) = sender ! response 
  }
  class NetworkResponder(sender: Node, request: PluginMessage) extends Responder {
    def apply(response: Response) = dhtController ! createResponseMessage(response)(request, sender.address)
  }
  
  class SearchOperation(
      val tid: TID,
      val searchString: String,
      val respond: Response => Any) {
    val pendingQueries: mutable.Set[Integer160] = HashSet(selfId)
    val reportedResults: mutable.Set[String] = HashSet()
    val pendingResults: mutable.Set[ContentRef] = HashSet()    
    
    def finish() = respond(SearchFinished(searchString))
  }
  
  // utilities for controller communication

  class SearchPluginMessage(tid: TID, r: Response) extends PluginMessage(tid, selfId, pid, r.pickle.value)

  def createResponseMessage(r: Response)(implicit request: PluginMessage, remote: InetSocketAddress): SendPluginMessage = {
    SendPluginMessage(new SearchPluginMessage(request.tid, r), new Node(request.id, remote))
  }
}

object SearchPlugin {
  def props(selfId: Integer160, dhtController: ActorRef, indexManager: ActorRef, dhtTable: Reader) = 
    Props(classOf[SearchPlugin], selfId, Plugin.SearchPluginId, dhtController, indexManager, dhtTable)
  
  sealed trait SearchMessage

  sealed trait Command extends SearchMessage

  final case class Announce(item: ContentItem) extends Command
  final case class Lookup(searchString: String) extends Command

  sealed trait Response extends SearchMessage

  //final case class AnnounceResult(itemId: CID, result: IndexManager.OfferResponse) extends Response
  final case class FoundItems(searchString: String, found: Traversable[ContentRef]) extends Response
  
  final case class SearchFinished(searchString: String) extends Response

  // XXX: incorporate this into DHT error handling mechanism
  final case class Error(code: Int, text: String) extends Response
  
  class IndexManagerActor(indexManager: IndexManager) extends Actor with ActorLogging {
    import IndexManagerActor._
    val system = context.system
    import system.dispatcher
    
    override def receive = {
      case IndexManagerCommand(id, cmd) => cmd match {
        case Announce(item) =>
          indexManager.offer(item)
        case Lookup(searchString) =>
          sender ! IndexManagerResponse(id, FoundItems(searchString, indexManager.search(searchString)))           
      }
      case ScheduleCleanup =>
        log.debug("ScheduleCleanup")
        if (indexManager.cleanupNeeded) {
          context.system.scheduler.scheduleOnce(15 seconds, self, Cleanup)
        }
      case Cleanup => {
        log.debug("Cleanup")
        indexManager.cleanup()
        context.system.scheduler.scheduleOnce(1 day, self, Cleanup)        
      }
      case Clear =>
        log.debug("Clear")
        indexManager.clear()
    }
    
    // schedule cleanup routine
    case object Cleanup
    case object ScheduleCleanup
    
    self ! ScheduleCleanup
  }
  object IndexManagerActor {
    case class IndexManagerCommand(id: TID, cmd: Command)
    case class IndexManagerResponse(id: TID, response: Response)
    case object Clear
  }
}

object PickleApp extends App {
  import SearchPlugin._

  import scala.pickling._
  import scala.pickling.binary._

  val a = Announce(new ContentItem(
    Integer160.random.toString,
    "Not very long title with year 2014",
    "Also not very long description, we should expect it to be way much longer",
    1024))

  val l = Lookup("futurama")

  val coms: List[Command] = List(a, l)

  val pp = coms.pickle

  val vv = pp.value

  println("size: " + vv.length + ", data: " + vv)

  vv.unpickle[List[Command]].foreach {
    case Announce(item) => println("announce: " + item)
    case Lookup(text) => println("lookup :" + text)
  }
}