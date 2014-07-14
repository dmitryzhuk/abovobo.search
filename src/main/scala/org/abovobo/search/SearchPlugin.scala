package org.abovobo.search

import akka.actor.ActorLogging
import org.abovobo.dht.Controller
import org.abovobo.integer.Integer160
import org.abovobo.dht
import Controller.SendPluginMessage
import org.abovobo.dht.TID
import org.abovobo.dht.PID
import org.abovobo.dht.Node
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

class SearchPlugin(
    selfIdGetter: () => Integer160, 
    dhtController: ActorRef, 
    indexManager: ActorRef, 
    dhtNodes: () => Traversable[Node]) extends Actor with ActorLogging {

  val system = this.context.system
  import system.dispatcher
  import SearchPlugin._
  
  val random = new Random()
  
  def selfId = selfIdGetter()

  val currentRequests: TransactionManager[TID, SearchOperation] =
    new TransactionManager[TID, SearchOperation](this.context.system.scheduler, 5.seconds, { (id) => self ! SearchTimeout(id) })
  val tidFactory = TIDFactory.random

  override def receive = {
    //
    // Message from network
    ///
    case Controller.Received(message: dht.message.Plugin, remote) =>
      val searchMessage = try { 
        deserializeMessage(message.payload)
      } catch {
        case e: Throwable =>
          log.warning("Cannot parse network message: \n{}\nException: {}", message.payload.toString(), e)
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
              case Error(code, error) =>
                log.error("error message from " + message.id + "@" + remote + ": " + error)
                search.finishForNode(message.id)
            }
          case None => log.warning("unexpected network response from " + remote + " for unknown/expired request: " + message.tid + ", " + response)
        }
        case _ => throw new IllegalStateException("command should be always wrapped into network command")
      }

    // 
    // Messages from local service user
    // 
    case Announce(item, params) => announce(selfId, item, params)
    
    case Lookup(searchString, params) => SearchOperation.start(selfId,searchString, params, new DirectResponder(this.sender()))
    
    case ClearIndex => indexManager ! IndexManagerActor.Clear

    // 
    // Messages from internal services/self
    //
    case IndexManagerActor.IndexManagerResponse(tid, response) =>
      currentRequests.get(tid) match {
        case Some(search) =>
          log.debug("Local response: " + response)
          response match {
            case FoundItems(searchString, refs) => search.addResults(selfId, refs)
            case Error(code, message) => log.error("Error from local index: " + code + ", " + message)
            case SearchFinished(searchString) => // shouldn't happen
          }
          search.finishForNode(selfId) // there will be no SearchFinished
        case None => log.info("local response for unknown/expired request: " + tid + ", " + response)
      }      

    case SearchTimeout(tid) => currentRequests.fail(tid) foreach(_.timeout())
  }
  
  def announce(from: Integer160, item: ContentItem, params: AnnounceParams) {
    indexManager ! IndexManagerActor.IndexManagerCommand(tidFactory.next(), Announce(item, params))
    if (!params.lastStop)  {
      val msg = Announce(item.compress, params.aged)
      randomNodesExcept(params.width, from) foreach { n =>
        val tid = tidFactory.next()
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
    
    private val pendingNodes: mutable.Set[Integer160] = mutable.HashSet(selfId)
    private val reportedResults: mutable.Set[String] = mutable.HashSet()
    //private val pendingResults: mutable.Set[ContentRef] = mutable.HashSet()

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
  
  class SearchPluginMessage(tid: TID, msg: SearchMessage) extends dht.message.Plugin(tid, selfId, SearchPlugin.PluginId, serializeMessage(msg))

  def createResponseMessage(tid: TID, to: Node, msg: Response) = SendPluginMessage(new SearchPluginMessage(tid, msg), to)
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
    dhtNodes: () => Traversable[Node],
    maxItemsCount: Int = SearchPlugin.DefaultMaxItemsCount): ActorRef = {
  
    val indexDir = homeDir.resolve("index")
    indexDir.toFile.mkdirs
    
    val index = new LuceneContentIndex(indexDir)
    
    val indexManager = new IndexManager(index, new IndexManagerRegistry("jdbc:h2:" + homeDir.resolve(name)), maxItemsCount)

    val indexManagerActor = parent.actorOf(Props(classOf[IndexManagerActor], indexManager), name + "-im")
    
    val searchPluginActor = parent.actorOf(props(selfId, messageChannel, indexManagerActor, dhtNodes))
    
    // subscribe for messages
    messageChannel ! Controller.PutPlugin(SearchPlugin.PluginId, searchPluginActor)
    
    searchPluginActor
  }

  def props(selfId: () => Integer160, messageChannel: ActorRef, indexManager: ActorRef, dhtNodes: () => Traversable[Node]) =
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
  
  
  // utilities for controller communication
  def deserializeMessage(msg: ByteString): SearchMessage = {
    val events = Bencode.decode(msg)

    def xthrow(code: Int, message: String) = throw new RuntimeException("error " + code + ": " +  message)

    def array(event: Bencode.Event): Array[Byte] = event match {
      case Bencode.Bytestring(value) => value
      case _ => xthrow(0, "Malformed packet")
    }

    /*
    def integer160(event: Bencode.Event): Integer160 = event match {
      case Bencode.Bytestring(value) => new Integer160(value)
      case _ => xthrow(0, "Malformed packet")
    }
    */

    def string(event: Bencode.Event): String = event match {
      case Bencode.Bytestring(value) => new String(value, "UTF-8")
      case _ => xthrow(0, "Malformed packet")
    }

    /*
    def byteString(event: Bencode.Event): ByteString = event match {
      case Bencode.Bytestring(value) => ByteString(value) 
      case _ => xthrow(0, "Malformed packet")      
    }
    */
    
    def integer(event: Bencode.Event): Long = event match {
      case Bencode.Integer(value) => value
      case _ => xthrow(0, "Malformed packet")
    }
    
    def next(): Bencode.Event = events.next()
    
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
    buf.result()
  }
  
}
