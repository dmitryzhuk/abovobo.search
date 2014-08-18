package org.abovobo.search

import akka.actor.ActorLogging
import org.abovobo.dht.controller.Controller
import org.abovobo.dht.Agent
import org.abovobo.integer.Integer160
import org.abovobo.dht
import Controller.SendPluginMessage
import org.abovobo.dht.TID
import org.abovobo.dht.PID
import org.abovobo.dht.NodeInfo
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.collection.mutable
import org.abovobo.dht.TIDFactory
import akka.actor.Actor
import akka.actor.Props
import scala.util.Random
import akka.util.ByteStringBuilder
import akka.util.ByteString
import java.nio.charset.Charset
import org.abovobo.conversions.Bencode
import org.abovobo.search.ContentIndex._
import java.nio.file.Path
import akka.actor.ActorRefFactory
import org.abovobo.search.impl.LuceneContentIndex
import org.abovobo.search.impl.H2IndexManagerRegistry
import org.abovobo.logging.global.GlobalLogger
import org.slf4j.helpers.NOPLogger


class SearchPlugin(
    selfIdGetter: () => Integer160, 
    messageSink: ActorRef, 
    indexManager: ActorRef, 
    dhtNodes: () => Traversable[NodeInfo]) extends Actor with ActorLogging {

  val system = this.context.system
  import system.dispatcher
  import SearchPlugin._
  
  val random = new Random()
  val glogger = NOPLogger.NOP_LOGGER //GlobalLogger.getLogger("search")
  
  def selfId = selfIdGetter()

  val currentRequests: TransactionManager[TID, SearchOperation] =
    new TransactionManager[TID, SearchOperation](this.context.system.scheduler, 5.seconds, { (id) => self ! SearchTimeout(id) })
  val tidFactory = TIDFactory.random

  override def receive = {
    //
    // Message from network
    ///
    case Agent.Received(message: dht.message.Plugin, remote) =>
      val searchMessage = try { 
        SearchMessagesSerialization.deserializeMessage(message.payload)
      } catch {
        case e: Throwable =>
          log.warning("Cannot parse network message: \n{}\nException: {}", message.payload.toString(), e)
          throw e
      }
      log.info("Got network message from " + remote + ": "  + searchMessage)
      
      glogger.trace("Got message {}", SearchLogMessage.in(selfId, searchMessage, message.id))
      
      searchMessage match {
        case Announce(item, params) => announce(message.id, item, params)

        case Lookup(searchString, params) => SearchOperation.start(message.id, searchString, params, new NetworkResponder(message.tid, new NodeInfo(message.id, remote)))
       
        case response: Response => currentRequests.get(message.tid) match {
          case Some(search) =>
            response match {
              case FoundItems(searchString, items) => search.addResults(message.id, items)
              case SearchFinished(searchString) => search.finishForNode(message.id)
              case Error(code, error) =>
                log.error("error message from " + message.id + "@" + remote + ": " + error)
                search.finishForNode(message.id)
            }
          case None => log.warning("unexpected network response from " + remote + " for unknown/expired request: " + message.tid + ", " + response)
        }
        
        case FloodClearIndex => FloodIndexCleaner.onClear()
        
        case _ => throw new IllegalStateException("command should be always wrapped into network command")
      }

    // 
    // Messages from local service user
    // 
    case Announce(item, params) => announce(selfId, item, params)
    
    case Lookup(searchString, params) => SearchOperation.start(selfId,searchString, params, new DirectResponder(this.sender()))
    
    case ClearIndex => indexManager ! IndexManagerActor.Clear
    
    case FloodClearIndex => FloodIndexCleaner.onClear()

    // 
    // Messages from internal services/self
    //
    case IndexManagerActor.IndexManagerResponse(tid, response) =>
      currentRequests.get(tid) match {
        case Some(search) =>
          log.debug("Local response: " + response)
          /*
          response match {
            case FoundItems(searchString, refs) => search.addResults(selfId, refs)
            case Error(code, message) => log.error("Error from local index: " + code + ", " + message)
            case SearchFinished(searchString) => // shouldn't happen
          }
          */
          search.finishForNode(selfId) // there will be no SearchFinished
        case None => log.info("local response for unknown/expired request: " + tid + ", " + response)
      }      

    case SearchTimeout(tid) => currentRequests.fail(tid) foreach(_.timeout())
  }
  
  def announce(from: Integer160, item: ContentItem, params: AnnounceParams) {
    val tid = tidFactory.next()
    indexManager ! IndexManagerActor.IndexManagerCommand(tid, Announce(item, params))
    if (!params.lastStop)  {
      val msg = Announce(item.compress, params.aged)
      randomNodesExcept(params.width, from) foreach { n =>
        log.info("Sending announce for " + item + " to " + n)        
        networkSend(tid, n, msg)
      }
    }
  }
    
  def randomNodesExcept(count: Int, id: Integer160) = {
    randomNodes(count + 1).filter(_.id != id).take(count)
  }
  
  def randomNodes(count: Int): Traversable[NodeInfo] = {
    random.shuffle(dhtNodes()).take(count)
  }
  
  trait Responder extends (Response => Any) 

  class DirectResponder(sender: ActorRef) extends Responder {
    def apply(response: Response) = sender ! response 
  }
  class NetworkResponder(tid: TID, sender: NodeInfo) extends Responder {
    def apply(response: Response) = {
      log.info("Sending search response " + response + " to " + sender)
      networkSend(tid, sender, response)
    }
  }

  
  private def networkSend(to: NodeInfo, spm: SearchPluginMessage) = {
    glogger.trace("Sending message {}", SearchLogMessage.out(selfId, spm.searchMessage, to.id))
    
    messageSink ! SendPluginMessage(spm, to)
  }
  private def networkSend(tid: TID, to: NodeInfo, msg: SearchMessage) = {
    glogger.trace("Sending message {}", SearchLogMessage.out(selfId, msg, to.id))
    
    messageSink ! SendPluginMessage(new SearchPluginMessage(tid, msg), to)
  }
  
  class SearchOperation private (
      val requesterId: Integer160,
      val tid: TID,
      val searchString: String,
      respond: Response => Any) {
    
    private val pendingNodes: mutable.Set[Integer160] = mutable.HashSet(selfId)
    private val reportedResults: mutable.Set[String] = mutable.HashSet()

    private def searchInNetwork(params: SearchParams): Traversable[NodeInfo] = {
      val msg = new SearchPluginMessage(tid, Lookup(searchString, params))
      
      val nodes = randomNodesExcept(params.width, requesterId)
      nodes foreach { n => 
        log.info("Sending search request for '" + searchString + "' to " + n)
        networkSend(n, msg)
      }
      nodes
    }
    
    def addResults(from: Integer160, items: scala.collection.Set[ContentRef]) = {
      val uniqueResults = items.filterNot { i => reportedResults.contains(i.id) }
  
      /// !!! TODO: use search.pendingResults to accumulate results here and fire them in bunches to requester (like each 1 second, or something else) to avoid excessive traffic
      
      if (uniqueResults.nonEmpty) {
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
    def start(requesterId: Integer160, searchString: String, params: SearchParams, responder: Responder): SearchOperation = {
      val tid = tidFactory.next()
      val search = new SearchOperation(requesterId, tid, searchString, responder)
  
      currentRequests.add(tid, search)
      
      indexManager ! IndexManagerActor.IndexManagerCommand(tid, Lookup(searchString, params))
      
      if (!params.lastStop) {
        search.pendingNodes ++= search.searchInNetwork(params.aged).map(_.id)
      }
      search
    }
  }
  
  class SearchPluginMessage(tid: TID, protected[SearchPlugin] val searchMessage: SearchMessage) extends dht.message.Plugin(tid, selfId, SearchPlugin.PluginId, SearchMessagesSerialization.serializeMessage(searchMessage))
  
  // Testing utilities 
  object FloodIndexCleaner {
    private val threshold = 5000L
    private var lastClean = 0L
    
    def onClear() {
      log.info("FloodIndexCleaner.onClear()")
      if (now - lastClean > threshold) {
        lastClean = now
        log.info("Starting flood clean!!!")
        
        indexManager ! IndexManagerActor.Clear

        dhtNodes() foreach { n => networkSend(tidFactory.next(), n, FloodClearIndex) }
      }
    }
    
    private def now = System.currentTimeMillis
  }  
}

object SearchPlugin {  
  val PluginId = new PID(1)
  val DefaultMaxItemsCount = 10000
  
  def apply(
    homeDir: Path,
    name: String, 
    parent: ActorRefFactory, 
    messageChannel: ActorRef,
    selfId: () => Integer160, 
    dhtNodes: () => Traversable[NodeInfo],
    maxItemsCount: Int = SearchPlugin.DefaultMaxItemsCount): ActorRef = {
  
    val indexDir = homeDir.resolve("index")
    indexDir.toFile.mkdirs
    
    val index = new LuceneContentIndex(indexDir)
        
    val imRegistry = H2IndexManagerRegistry("jdbc:h2:" + homeDir.resolve(name))
    
    val indexManager = new IndexManager(index, imRegistry, maxItemsCount)

    val indexManagerActor = parent.actorOf(Props(classOf[IndexManagerActor], indexManager), name + "-im")
    
    val searchPluginActor = parent.actorOf(props(selfId, messageChannel, indexManagerActor, dhtNodes))
    
    // subscribe for messages
    messageChannel ! Controller.PutPlugin(SearchPlugin.PluginId, searchPluginActor)
    
    searchPluginActor
  }

  def props(selfId: () => Integer160, messageChannel: ActorRef, indexManager: ActorRef, dhtNodes: () => Traversable[NodeInfo]) =
    Props(classOf[SearchPlugin], selfId, messageChannel, indexManager, dhtNodes)

  trait Params {
    /** 
     *  time to live
     *  
     *  1 means only this node (i.e. request will not be send over the network)
     *  2 means this not and 'width' random nodes from network 
     *  .. and so on
     */
    def ttl: Int
    
    /**
     * search with - how many random nodes from routing table to contact on each hop
     */
    def width: Int
    def lastStop = ttl <= 1    
  }
  
  sealed case class AnnounceParams(ttl: Int, width: Int) extends Params {
    if (ttl <= 0) throw new IllegalArgumentException("Zombies here!")
    if (width < 0) throw new IllegalArgumentException
    def aged = new AnnounceParams(ttl - 1, width)
  }  
  sealed case class SearchParams(ttl: Int, width: Int) extends Params {
    if (ttl <= 0) throw new IllegalArgumentException("Zombies here!") 
    if (width < 0) throw new IllegalArgumentException
    
    def aged = new SearchParams(ttl - 1, width)    
  }
  
  val DefaultAnnounceParams = new AnnounceParams(3, 5)
  val DefaultSearchParams = new SearchParams(3, 5)
  
  sealed trait SearchMessage
  
  object ClearIndex extends SearchMessage
  
  object FloodClearIndex extends SearchMessage

  sealed trait Command extends SearchMessage

  final case class Announce(item: ContentItem, params: AnnounceParams = DefaultAnnounceParams) extends Command
  final case class Lookup(searchString: String, params: SearchParams = DefaultSearchParams) extends Command {
    if (searchString.length > 128) throw new IllegalArgumentException("Search string is too long")
  }

  sealed trait Response extends SearchMessage

  //final case class AnnounceResult(itemId: CID, result: IndexManager.OfferResponse) extends Response
  final case class FoundItems(searchString: String, found: scala.collection.Set[ContentRef]) extends Response
  
  final case class SearchFinished(searchString: String) extends Response

  // XXX: incorporate this into DHT error handling mechanism
  final case class Error(code: Int, text: String) extends Response
  
  // private messages
  case class SearchTimeout(tid: TID)  
  
}
