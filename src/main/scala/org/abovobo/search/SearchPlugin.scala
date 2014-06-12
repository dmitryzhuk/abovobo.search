package org.abovobo.search

import org.abovobo.dht.Plugin
import org.abovobo.dht.Controller
import org.abovobo.dht.persistence.Reader
import org.abovobo.dht.PluginMessage
import akka.actor.ActorLogging
import org.abovobo.search.ContentIndex.ContentItem
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
import scala.util.Random
import scala.collection.mutable.ArraySeq
import scala.collection.mutable.ArraySeq
import akka.serialization.SerializationExtension

class SearchPlugin(selfId: Integer160, pid: Plugin.PID, dhtController: ActorRef, indexManager: ActorRef, dhtNodes: () => Traversable[Node]) extends Plugin(pid) with ActorLogging {
  //import scala.pickling._
  //import scala.pickling.json._
  //import scala.pickling.binary._
  import SearchPlugin._

  val system = this.context.system
  import system.dispatcher
  import SearchPlugin._
  
  val announceWidth = 10
  val announceTtl = 3
  val random = new Random()

  val currentRequests: TransactionManager[TID, SearchOperation] = new TransactionManager(this.context.system.scheduler, 5.seconds, { (id) => self ! SearchTimeout(id) })
  val tidFactory = TIDFactory.random

  override def receive = {
    //
    // Message from network
    ///
    case Controller.Received(message: PluginMessage, remote) => {
      val searchMessage = try { 
        
        deserializeMessage(message.payloadBytes)
         
        //new String(message.payloadBytes, "UTF-8").unpickle[SearchMessage]
        
      } catch {
        case e: Throwable =>
          log.warning("Cannot parse network message: \n{}\nException: {}", new String(message.payloadBytes), e)
          throw e
      }
      log.debug("Got network message from " + remote + ": "  + searchMessage)
      
      searchMessage match {
        case SearchNetworkCommand(cmd, ttl) => cmd match {
          case Announce(item) => announce(message.id, item, ttl)
          case Lookup(searchString) => SearchOperation.start(message.id, searchString, new NetworkResponder(message.tid, new Node(message.id, remote)), ttl)
        }
       
        case response: Response => currentRequests.get(message.tid) match {
          case Some(search) =>
            response match {
              case FoundItems(searchString, items) => search.addResults(message.id, items)
              case SearchFinished(searchString) => search.finishForNode(message.id)
              case Error(code, error) => {
                log.error("error message from " + message.id + "@" + remote + ": " + error)
                search.finishForNode(message.id)
              }
            }
          case None => log.warning("unexpected network response from " + remote + " for unknown/expired request: " + message.tid + ", " + response)
        }
        case _ => throw new IllegalStateException("command should be always wrapped into network command")
      }
    }
    
    // 
    // Messages from local service user
    // 
    case Announce(item) => announce(selfId, item, announceTtl)
    
    case Lookup(searchString) => SearchOperation.start(selfId,searchString, new DirectResponder(sender))

    // 
    // Messages from internal services/self
    //
    case IndexManagerResponse(tid, response) => { 
      currentRequests.get(tid) match {
        case Some(search) => {
          log.debug("Local response: " + response)
          response match {
            case FoundItems(searchString, refs) => search.addResults(selfId, refs)
            case Error(code, message) => log.error("Error from local index: " + code + ", " + message)
            case SearchFinished(searchString) => // shouldn't happen
          }
          search.finishForNode(selfId)
        }
        case None => log.info("local response for unknown/expired request: " + tid + ", " + response)
      }      
    }
      
    case SearchTimeout(tid) => currentRequests.fail(tid) foreach(_.timeout)
  }
  
  def announce(from: Integer160, item: ContentItem, ttl: Int) {
    log.debug("Node " + selfId + " got announce: " + item)    
    
    indexManager ! IndexManagerCommand(tidFactory.next, Announce(item))
    if (ttl > 1)  {
      def announceToNetwork(item: ContentItem, ttl: Int) {
        val msg = SearchNetworkCommand(Announce(item), ttl.toByte)        
        randomNodesExcept(announceWidth, from) foreach { n =>
          val tid = tidFactory.next
          val pm = new SearchPluginMessage(tid, msg)      
          
          log.debug("Sending announce for " + item + " to " + n)
          
          dhtController ! SendPluginMessage(pm, n)
        }
      }      
      announceToNetwork(item, ttl - 1)
    }
  }
    
  def randomNodesExcept(count: Int, id: Integer160) = {
    randomNodes2(count + 1).filter(_.id != id).take(count)
  }
  
  def randomNodes2(count: Int): Traversable[Node] = {
    random.shuffle(dhtNodes()).take(count)
  }
  
  
  /// active searches state
  
  trait Responder extends (Response => Any) 

  class DirectResponder(sender: ActorRef) extends Responder {
    def apply(response: Response) = sender ! response 
  }
  class NetworkResponder(tid: TID, sender: Node) extends Responder {
    def apply(response: Response) = {
      log.debug("Sending search response " + response + " to " + sender)
      dhtController ! createResponseMessage(tid, sender, response)
    }
  }
  
  class SearchOperation private (
      val requesterId: Integer160,
      val tid: TID,
      val searchString: String,
      respond: Response => Any) {
    
    private val pendingNodes: mutable.Set[Integer160] = HashSet(selfId)
    private val reportedResults: mutable.Set[String] = HashSet()
    private val pendingResults: mutable.Set[ContentRef] = HashSet()    

    private def searchInNetwork(ttl: Int): Traversable[Node] = {
      val msg = new SearchPluginMessage(tid, SearchNetworkCommand(Lookup(searchString), ttl.toByte))
      
      val nodes = randomNodesExcept(SearchOperation.SearchWidth, requesterId)
      nodes foreach { n => 
        log.debug("Sending search request for " + searchString + " to " + n)
        dhtController ! SendPluginMessage(msg, n)
      }
      nodes
    }
    
    def addResults(from: Integer160, items: Set[ContentRef]) = {
      val uniqueResults = items.filterNot { i => reportedResults.contains(i.id) }
  
      /// !!! TODO: use search.pendingResults to accumulate results here and fire them in bunches to requester (like each 1 second, or something else) to avoid excessive traffic
      
      if (!uniqueResults.isEmpty) {
        respond(FoundItems(searchString, uniqueResults))
        reportedResults ++= uniqueResults.map(_.id)      
      }
    }
    
    def finishForNode(node: Integer160) {      
      if ((pendingNodes -= node).isEmpty) {
        finish()
        currentRequests.complete(tid)
      }    
    } 
    
    def finish() { respond(SearchFinished(searchString)) }
    
    def timeout() {
        log.debug("finishing search " + tid + " " + searchString + " by timeout. Not responded: " + pendingNodes.size)
        finish()
    }    
  }
  
  object SearchOperation {
    val SearchTtl = 3
    val SearchWidth = 10
        
    def start(requesterId: Integer160, searchString: String, responder: Responder, ttl: Int = SearchTtl): SearchOperation = {
      val tid = tidFactory.next
      val search = new SearchOperation(requesterId, tid, searchString, responder)
  
      currentRequests.add(tid, search)
      
      indexManager ! IndexManagerCommand(tid, Lookup(searchString))
      
      if (ttl > 1) {
        val queriedNodes = search.searchInNetwork(ttl - 1).map(_.id)
        search.pendingNodes ++= queriedNodes
      }
      search
    }
  }
  
  val serialization = SerializationExtension(system)
  val serializer = serialization.findSerializerFor(Lookup("dummy"))

  // utilities for controller communication
  def deserializeMessage(arr: Array[Byte]): SearchMessage = {
    serializer.fromBinary(arr).asInstanceOf[SearchMessage]
  }
  
  def serializeMessage(msg: SearchMessage): Array[Byte] = {
    // somehow this call breaks jvm code verification when inline...
    //msg.pickle.value.getBytes("UTF-8") 
    serializer.toBinary(msg)
  }

  class SearchPluginMessage(tid: TID, msg: SearchMessage) extends PluginMessage(tid, selfId, pid, serializeMessage(msg))

  def createResponseMessage(tid: TID, to: Node, msg: Response) = SendPluginMessage(new SearchPluginMessage(tid, msg), to)
}

object SearchPlugin {
  def props(selfId: Integer160, dhtController: ActorRef, indexManager: ActorRef, dhtNodes: () => Traversable[Node]) = 
    Props(classOf[SearchPlugin], selfId, Plugin.SearchPluginId, dhtController, indexManager, dhtNodes)
  
  sealed trait SearchMessage

  sealed trait Command extends SearchMessage

  final case class Announce(item: ContentItem) extends Command
  final case class Lookup(searchString: String) extends Command

  sealed trait Response extends SearchMessage

  //final case class AnnounceResult(itemId: CID, result: IndexManager.OfferResponse) extends Response
  final case class FoundItems(searchString: String, found: Set[ContentRef]) extends Response
  
  final case class SearchFinished(searchString: String) extends Response

  // XXX: incorporate this into DHT error handling mechanism
  final case class Error(code: Int, text: String) extends Response
  
  class IndexManagerActor(indexManager: IndexManager) extends Actor with ActorLogging {
    import IndexManagerActor._
    val system = context.system
    import system.dispatcher
    
    override def receive = {
      case IndexManagerCommand(id, cmd) => cmd match {
        case Announce(item) => indexManager.offer(item)
        case Lookup(searchString) => sender ! IndexManagerResponse(id, FoundItems(searchString, indexManager.search(searchString)))           
      }
      case ScheduleCleanup =>
        if (indexManager.cleanupNeeded) {
          context.system.scheduler.scheduleOnce(30 seconds, self, Cleanup)
        }
      case Cleanup => {
        indexManager.cleanup()
        context.system.scheduler.scheduleOnce(1 day, self, Cleanup)        
      }
      case Clear => indexManager.clear()
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
  
    // private messages
  case class SearchNetworkCommand(cmd: Command, ttl: Byte) extends SearchMessage
  case class SearchTimeout(tid: TID)
}

//object PickleApp extends App {
//  import SearchPlugin._
//
//  import scala.pickling._
//  import scala.pickling.binary._
//
//  val a = Announce(new ContentItem(
//    Integer160.random.toString,
//    "Not very long title with year 2014",
//    "Also not very long description, we should expect it to be way much longer",
//    1024))
//
//  val l = Lookup("futurama")
//  
//  var coms: List[SearchMessage] = List(a, l)
//
//
//  for (i <- 1 to 6) {
//    var items = Array[ContentRef]()
//    for (j <- 0 until i) {
//      items = items ++ Array(ContentIndex.SimpleContentRef(Integer160.random.toString, "random title"))
//    }
//    
//    coms ::= FoundItems("test" + i, items)
//  }
//  
////  val items = List(
////      //new ContentIndex.SimpleContentRef(Integer160.random.toString, "random title"),
////      new ContentIndex.SimpleContentRef(Integer160.random.toString, "random title 2")
////      )
//  
//  //val f = FoundItems("test", items)
//
//  val pp = coms.pickle
//
//  val vv = pp.value
//
//  println("size: " + vv.length + ", data: " + new String(vv))
//
//  vv.unpickle[List[SearchMessage]].foreach {
//    case Announce(item) => println("announce: " + item)
//    case Lookup(text) => println("lookup :" + text)
//    case FoundItems(text, items) => println("found: " + items)
//  }
//}