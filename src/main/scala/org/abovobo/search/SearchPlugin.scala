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
import akka.util.ByteStringBuilder
import akka.util.ByteString
import java.nio.charset.Charset
import org.abovobo.conversions.Bencode
import org.abovobo.search.ContentIndex._

class SearchPlugin(selfId: Integer160, pid: Plugin.PID, dhtController: ActorRef, indexManager: ActorRef, dhtNodes: () => Traversable[Node]) extends Plugin(pid) with ActorLogging {
  import SearchPlugin._

  val system = this.context.system
  import system.dispatcher
  import SearchPlugin._
  
  val random = new Random()

  val currentRequests: TransactionManager[TID, SearchOperation] = new TransactionManager(this.context.system.scheduler, 5.seconds, { (id) => self ! SearchTimeout(id) })
  val tidFactory = TIDFactory.random

  override def receive = {
    //
    // Message from network
    ///
    case Controller.Received(message: PluginMessage, remote) => {
      val searchMessage = try { 
        deserializeMessage(message.payload)
      } catch {
        case e: Throwable =>
          log.warning("Cannot parse network message: \n{}\nException: {}", message.payload.toString, e)
          throw e
      }
      log.info("Got network message from " + remote + ": "  + searchMessage)
      
      searchMessage match {
        case Announce(item, params) => announce(message.id, item, params)

        case Lookup(searchString, params) => SearchOperation.start(message.id, searchString, params, new NetworkResponder(message.tid, new Node(message.id, remote)))
       
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
    case Announce(item, params) => announce(selfId, item, params)
    
    case Lookup(searchString, params) => SearchOperation.start(selfId,searchString, params, new DirectResponder(sender))

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
          search.finishForNode(selfId) // there will be no SearchFinished
        }
        case None => log.info("local response for unknown/expired request: " + tid + ", " + response)
      }      
    }
      
    case SearchTimeout(tid) => currentRequests.fail(tid) foreach(_.timeout)
  }
  
  def announce(from: Integer160, item: ContentItem, params: AnnounceParams) {
    indexManager ! IndexManagerCommand(tidFactory.next, Announce(item, params))
    if (!params.lastStop)  {
      val msg = Announce(item.compress, params.aged)
      randomNodesExcept(params.width, from) foreach { n =>
        val tid = tidFactory.next
        val pm = new SearchPluginMessage(tid, msg)      
        
        log.info("Sending announce for " + item + " to " + n)
        
        dhtController ! SendPluginMessage(pm, n)
      }
    }
  }
    
  def randomNodesExcept(count: Int, id: Integer160) = {
    randomNodes(count + 1).filter(_.id != id).take(count)
  }
  
  def randomNodes(count: Int): Traversable[Node] = {
    random.shuffle(dhtNodes()).take(count)
  }
  
  trait Responder extends (Response => Any) 

  class DirectResponder(sender: ActorRef) extends Responder {
    def apply(response: Response) = sender ! response 
  }
  class NetworkResponder(tid: TID, sender: Node) extends Responder {
    def apply(response: Response) = {
      log.info("Sending search response " + response + " to " + sender)
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

    private def searchInNetwork(params: SearchParams): Traversable[Node] = {
      val msg = new SearchPluginMessage(tid, Lookup(searchString, params))
      
      val nodes = randomNodesExcept(params.width, requesterId)
      nodes foreach { n => 
        log.info("Sending search request for '" + searchString + "' to " + n)
        dhtController ! SendPluginMessage(msg, n)
      }
      nodes
    }
    
    def addResults(from: Integer160, items: scala.collection.Set[ContentRef]) = {
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
    def start(requesterId: Integer160, searchString: String, params: SearchParams, responder: Responder): SearchOperation = {
      val tid = tidFactory.next
      val search = new SearchOperation(requesterId, tid, searchString, responder)
  
      currentRequests.add(tid, search)
      
      indexManager ! IndexManagerCommand(tid, Lookup(searchString, params))
      
      if (!params.lastStop) {
        search.pendingNodes ++= search.searchInNetwork(params.aged).map(_.id)
      }
      search
    }
  }
  
  class SearchPluginMessage(tid: TID, msg: SearchMessage) extends PluginMessage(tid, selfId, pid, serializeMessage(msg))

  def createResponseMessage(tid: TID, to: Node, msg: Response) = SendPluginMessage(new SearchPluginMessage(tid, msg), to)
}

object SearchPlugin {
  def props(selfId: Integer160, dhtController: ActorRef, indexManager: ActorRef, dhtNodes: () => Traversable[Node]) = 
    Props(classOf[SearchPlugin], selfId, Plugin.SearchPluginId, dhtController, indexManager, dhtNodes)

  trait Params {
    /** 
     *  time to live
     *  
     *  1 means only this node (i.e. request will not be send over the network)
     *  2 means this not and 'width' random nodes from network and so on
     */
    def ttl: Int
    
    /**
     * search with - how many random nodes from routing table to contact of each hop
     */
    def width: Int
    def lastStop = ttl <= 1    
  }
  
  sealed case class AnnounceParams(ttl: Int, width: Int) extends Params {
    if (ttl <= 0) throw new IllegalArgumentException("Zombies here!")
    if (width <= 0) throw new IllegalArgumentException
    def aged = new AnnounceParams(ttl - 1, width)
  }  
  sealed case class SearchParams(ttl: Int, width: Int) extends Params {
    if (ttl <= 0) throw new IllegalArgumentException("Zombies here!") 
    if (width <= 0) throw new IllegalArgumentException
    
    def aged = new SearchParams(ttl - 1, width)    
  }
  
  val DefaultAnnounceParams = new AnnounceParams(3, 5)
  val DefaultSearchParams = new SearchParams(3, 5)
  
  sealed trait SearchMessage

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
        case Lookup(searchString, _) => sender ! IndexManagerResponse(id, FoundItems(searchString, indexManager.search(searchString))) // Note: there will be no SearchFinished message!
      }
      case ScheduleCleanup =>
        if (indexManager.cleanupNeeded) {
          context.system.scheduler.scheduleOnce(30.seconds, self, Cleanup)
        }
      case Cleanup => {
        indexManager.cleanup()
        context.system.scheduler.scheduleOnce(1.day, self, Cleanup)
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
  case class SearchTimeout(tid: TID)
  
  
  // utilities for controller communication
  def deserializeMessage(msg: ByteString): SearchMessage = {
    var events = Bencode.decode(msg)

    def xthrow(code: Int, message: String) = throw new RuntimeException("error " + code + ": " +  message)

    def array(event: Bencode.Event): Array[Byte] = event match {
      case Bencode.Bytestring(value) => value
      case _ => xthrow(0, "Malformed packet")
    }

    def integer160(event: Bencode.Event): Integer160 = event match {
      case Bencode.Bytestring(value) => new Integer160(value)
      case _ => xthrow(0, "Malformed packet")
    }

    def string(event: Bencode.Event): String = event match {
      case Bencode.Bytestring(value) => new String(value, "UTF-8")
      case _ => xthrow(0, "Malformed packet")
    }
    
    def byteString(event: Bencode.Event): ByteString = event match {
      case Bencode.Bytestring(value) => ByteString(value) 
      case _ => xthrow(0, "Malformed packet")      
    }
    
    def integer(event: Bencode.Event): Long = event match {
      case Bencode.Integer(value) => value
      case _ => xthrow(0, "Malformed packet")
    }
    
    def next(): Bencode.Event = events.next
    
    def ensure(ec: Class[_ <: Bencode.Event]) = {
      val n = next()
      if (!ec.isInstance(n)) {
        xthrow(2, "Illegal arguments")         
      }
    }
    
    ensure(classOf[Bencode.DictionaryBegin])
        
    string(next()) match {
      case "a" => // Announce
        next() // list
        val id = string(next())
        val title = string(next())
        val size = integer(next())
        val description = array(next())
        val ttl = integer(next())
        val width = integer(next())
        ensure(classOf[Bencode.ListEnd])
        Announce(new CompressedContentItem(id, title, size, description), new AnnounceParams(ttl.toInt, width.toInt))
      case "l" => // Lookup
        next() // list
        val searchString = string(next())
        val ttl = integer(next())
        val width = integer(next())
        ensure(classOf[Bencode.ListEnd])
        Lookup(searchString, new SearchParams(ttl.toInt, width.toInt))
      case "f" => // FoundItems
        next() // list
        val searchString = string(next())
        // lists with items
        val items = scala.collection.mutable.Set.empty[ContentRef]
        while (next().isInstanceOf[Bencode.ListBegin]) {
          items += new SimpleContentRef(string(next()), string(next()), integer(next()))
          ensure(classOf[Bencode.ListEnd])
        }
        // ensure(classOf[Bencode.ListEnd]) - discarded in while
        FoundItems(searchString, items)
      case "s" => // SearchFinished
        SearchFinished(string(next()))
      case "e" => // Error
        next() // list
        val e = Error(integer(next()).toInt, string(next()))
        ensure(classOf[Bencode.ListEnd])
        e
      case x => xthrow(1, "Unknown message type " + x)
    }
  }
  
  def serializeMessage(msg: SearchMessage): ByteString = {
    val buf = new ByteStringBuilder()
    buf += 'd'
    
    val charset = Charset.forName("UTF-8")
    def putStr(str: String) {
      putArray(str.getBytes(charset))
    }
    def putNum(i: Long) {
      buf += 'i' ++= i.toString.getBytes(charset) += 'e'
    } 
    def putArray(bytes: Array[Byte]) {
      buf ++= bytes.length.toString.getBytes(charset) += ':' ++= bytes 
    }
    def putChar(ch: Byte) = {
      buf += '1' += ':' += ch
    }
      
    msg match {
      case Announce(item, params) => 
        putChar('a')
        buf += 'l'
          putStr(item.id)
          putStr(item.title)
          putNum(item.size)
          putArray(item.compress.descriptionData)
          putNum(params.ttl)
          putNum(params.width)
        buf += 'e'
      case Lookup(searchString, params) =>          
        putChar('l')
        buf += 'l'
          putStr(searchString)
          putNum(params.ttl)
          putNum(params.width)
        buf += 'e'          
      case FoundItems(str, found) =>
        putChar('f')
        buf += 'l'
          putStr(str)            
          found.foreach { item => 
            buf += 'l'
              putStr(item.id)
              putStr(item.title)
              putNum(item.size)
            buf += 'e'               
          }  
        buf += 'e' 
      case SearchFinished(str) =>
        putStr("s")
        putStr(str)
      case Error(code, text) =>
        putChar('e')
        buf += 'l'
          putNum(code)
          putStr(text)
        buf += 'e'
    }
    buf += 'e'  
    buf.result
  }
  
}
