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

class SearchPlugin(pid: Plugin.PID, dhtController: ActorRef, dhtTable: Reader, im: IndexManager) extends Plugin(pid) with ActorLogging {
  import scala.pickling._
  import scala.pickling.binary._

  import SearchPlugin._

  // XXX: to params
  private val selfId = Integer160.random

  // XXX: to params?
  private val transactionManager: TransactionManager[TID, Command] = null // = new TransactionManager(this.context.system.scheduler, 15.seconds)

  override def receive = {
    case Controller.Received(pluginMessage, remote) =>
      pluginMessage match {
        case message: PluginMessage => {
          implicit val pm = message
          implicit val rm = remote

          message.payloadBytes.unpickle[SearchMessage] match {
            case c: Command => {
              this.log.debug("Got command: " + c)
              // TODO: begin transaction
              c match {
                case Announce(item) => {
                  // XXX: we should respond to 'sender' somehow. probably we should introduce a proxy that will wrap the response into plugin message 
                  dhtController ! createResponseMessage(AnnounceResult(im.offer(item)))
                }
                case Lookup(searchString) => {
                  // local lookup
                  val found = im.search(searchString)
                  dhtController ! createResponseMessage(FoundItems(found))
                }
              }
            }
            case r: Response => {
              // TODO: end transaction 
              this.log.debug("Got response message: " + r)
              r match {
                case AnnounceResult(result) =>
                case FoundItems(found) =>
                case Error(code, text) =>
              }
            }
          }
        } // case PluginMessage
        case _ => this.log.error("Wrong message type recieved from DHT Controller")
      }
    case Announce(item) =>
      // local response
      sender ! AnnounceResult(im.offer(item))
    case Lookup(searchString) =>
      // local results
      sender ! FoundItems(im.search(searchString))


    // XXX: 
    // here it should be like
    // case error: Error => 
    // from Controller. Or something like that
  }

  class SearchPluginResponse(tid: TID, r: Response) extends PluginMessage(tid, selfId, pid, r.pickle.value)

  def createResponseMessage(r: Response)(implicit request: PluginMessage, remote: InetSocketAddress): SendPluginMessage = {
    SendPluginMessage(new SearchPluginResponse(request.tid, r), new Node(request.id, remote))
  }
}

object SearchPlugin {
  sealed trait SearchMessage

  sealed trait Command extends SearchMessage

  final case class Announce(item: ContentItem) extends Command
  final case class Lookup(searchString: String) extends Command

  sealed trait Response extends SearchMessage

  final case class AnnounceResult(result: IndexManager.OfferResponse) extends Response
  final case class FoundItems(found: Traversable[ContentRef]) extends Response

  // XXX: incorporate this into DHT error handling mechanism
  final case class Error(code: Int, text: String) extends Response
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