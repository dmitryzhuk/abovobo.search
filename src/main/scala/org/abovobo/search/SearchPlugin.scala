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
        deserializeMessage(message.payload)
      } catch {
        case e: Throwable =>
          log.warning("Cannot parse network message: \n{}\nException: {}", message.payload.toString, e)
          throw e
      }
      log.info("Got network message from " + remote + ": "  + searchMessage)
      
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
          search.finishForNode(selfId) // there will be no SearchFinished
        }
        case None => log.info("local response for unknown/expired request: " + tid + ", " + response)
      }      
    }
      
    case SearchTimeout(tid) => currentRequests.fail(tid) foreach(_.timeout)
  }
  
  def announce(from: Integer160, item: ContentItem, ttl: Int) {
    indexManager ! IndexManagerCommand(tidFactory.next, Announce(item))
    if (ttl > 1)  {
      def announceToNetwork(item: ContentItem, ttl: Int) {
        val msg = SearchNetworkCommand(Announce(item), ttl.toByte)        
        randomNodesExcept(announceWidth, from) foreach { n =>
          val tid = tidFactory.next
          val pm = new SearchPluginMessage(tid, msg)      
          
          log.info("Sending announce for " + item + " to " + n)
          
          dhtController ! SendPluginMessage(pm, n)
        }
      }      
      announceToNetwork(item.compress, ttl - 1)
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

    private def searchInNetwork(ttl: Int): Traversable[Node] = {
      val msg = new SearchPluginMessage(tid, SearchNetworkCommand(Lookup(searchString), ttl.toByte))
      
      val nodes = randomNodesExcept(SearchOperation.SearchWidth, requesterId)
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

  

  class SearchPluginMessage(tid: TID, msg: SearchMessage) extends PluginMessage(tid, selfId, pid, serializeMessage(msg))

  def createResponseMessage(tid: TID, to: Node, msg: Response) = SendPluginMessage(new SearchPluginMessage(tid, msg), to)
}

object SearchPlugin {
  def props(selfId: Integer160, dhtController: ActorRef, indexManager: ActorRef, dhtNodes: () => Traversable[Node]) = 
    Props(classOf[SearchPlugin], selfId, Plugin.SearchPluginId, dhtController, indexManager, dhtNodes)
  
  sealed trait SearchMessage

  sealed trait Command extends SearchMessage

  final case class Announce(item: ContentItem) extends Command
  final case class Lookup(searchString: String) extends Command {
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
        case Announce(item) => indexManager.offer(item)
        case Lookup(searchString) => sender ! IndexManagerResponse(id, FoundItems(searchString, indexManager.search(searchString))) // Note: there will be no SearchFinished message!
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
  
  
  // utilities for controller communication
  def deserializeMessage(msg: ByteString): SearchMessage = {
    //serializer.fromBinary(msg.toArray).asInstanceOf[SearchMessage]
    
    var events = Bencode.decode(msg)//.toIndexedSeq
    
    
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
        val ttl = integer(next())
        val id = string(next())
        val title = string(next())
        val size = integer(next())
        val description = array(next())
        ensure(classOf[Bencode.ListEnd])
        SearchNetworkCommand(Announce(new CompressedContentItem(id, title, size, description)), ttl.toByte)
      case "l" => // Lookup
        next() // list
        val ttl = integer(next())
        val searchString = string(next())
        ensure(classOf[Bencode.ListEnd])
        SearchNetworkCommand(Lookup(searchString), ttl.toByte)
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
      case SearchNetworkCommand(command, ttl) => command match {
        case Announce(item) => 
          putChar('a')
          buf += 'l'
            putNum(ttl)
            putStr(item.id)
            putStr(item.title)
            putNum(item.size)
            putArray(item.compress.descriptionData)
          buf += 'e'
        case Lookup(searchString) =>          
          putChar('l')
          buf += 'l'
            putNum(ttl)
            putStr(searchString)
          buf += 'e'          
      }
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
        
      case cmd: Command => throw new IllegalArgumentException("Commands should be wrapped to SearchNetworkCommand: " + cmd)
    }
    buf += 'e'  
    buf.result
  }
  
}
