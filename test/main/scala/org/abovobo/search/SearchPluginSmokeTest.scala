package org.abovobo.search

import java.net.InetSocketAddress
import java.net.Inet4Address
import java.net.InetAddress
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorLogging
import com.typesafe.config.Config
import com.typesafe.config.impl.SimpleConfig
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._
import akka.actor.Inbox
import akka.actor.Props
import akka.actor.ActorDSL._
import org.abovobo.dht.persistence.H2Storage
import org.abovobo.dht.persistence.Storage
import org.abovobo.dht.persistence.Reader
import org.abovobo.dht.persistence.Writer
import org.abovobo.arm.Disposable
import akka.actor.ActorRef
import org.abovobo.dht.Agent
import org.abovobo.dht.Table
import org.abovobo.dht.Controller
import org.abovobo.search.impl.InMemoryContentIndex
import org.abovobo.search.SearchPlugin._
import org.abovobo.search.ContentIndex.ContentItem
import org.abovobo.integer.Integer160
import akka.actor.ActorDSL._
import akka.actor.ActorSystem
import scala.concurrent.duration._
import org.abovobo.dht.Plugin
import java.util.concurrent.TimeoutException
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import org.abovobo.dht.Controller.PutPlugin



object SearchPluginSmokeTest extends App {
  val systemConfig = ConfigFactory.parseMap(Map(
      "akka.log-dead-letters" -> "true", 
      "akka.actor.debug.lifecycle" -> true,
      "akka.loglevel" -> "debug",
      
    "akka.actor.debug.receive" -> true,
    "akka.actor.debug.unhandled" -> true))
    
    
  def localEndpoint(ordinal: Int) = new InetSocketAddress(InetAddress.getLocalHost, 20000 + ordinal)

  case class NodeSystem(ordinal: Int, table: ActorRef, agent: ActorRef, controller: ActorRef, system: ActorSystem, storage: H2Storage) extends Disposable {
    val endpoint: InetSocketAddress = localEndpoint(ordinal)
    def dispose() {
        system.shutdown()
        system.awaitTermination()
        storage.close()     
    }
  }
  
    
  def createNode(ordinal: Int, routers: List[InetSocketAddress] = List()): NodeSystem = {
      val h2 = H2Storage.open("~/db/dht-" + ordinal, true) 

    val storage: Storage = h2
    val reader: Reader = h2
    val writer: Writer = h2
      
    val system = ActorSystem("TestSystem-" + ordinal, systemConfig)

    val agent = system.actorOf(Props(classOf[Agent], localEndpoint(ordinal), 10 seconds), "agent")
    val controller = system.actorOf(Controller.props(routers, reader, writer), "controller")
    val table = system.actorOf(Table.props(reader, writer), "table")    
  
    NodeSystem(ordinal, table, agent, controller, system, h2)
  }
    
  def createSearchPlugin(node: NodeSystem) = {
    val indexManager = node.system.actorOf(Props(classOf[IndexManagerActor], 
        new IndexManager(10, new InMemoryContentIndex(), new IndexManagerRegistry("jdbc:h2:~/db/search-" + node.ordinal))), "indexManager")
        
    val searchPlugin = node.system.actorOf(SearchPlugin.props(node.storage.id.get, node.controller, indexManager, node.storage), "search")

    indexManager ! IndexManagerActor.Clear 
    
    node.controller ! PutPlugin(Plugin.SearchPluginId, searchPlugin)
    
    searchPlugin
  }
    
  val node = createNode(1)

  Thread.sleep(5 * 1000)

  val node2 = createNode(2, List(node.endpoint))

  Thread.sleep(5 * 1000)

  val node3 = createNode(3, List(node.endpoint))
  
  
  val searchPlugin = createSearchPlugin(node)
  val searchPlugin2 = createSearchPlugin(node2)
  
  val id = Integer160.random.toString

  val timeoutDuration: FiniteDuration = 7 seconds
  implicit val timeout: Timeout = timeoutDuration
  val inbox = Inbox.create(node.system)

  def receive() = {
    try {
      inbox.receive(timeoutDuration)
    } catch {
      case e: TimeoutException => e
    }    
  }
  
  def announce(search: ActorRef, item: ContentItem) {
    //val res = Await.result(searchPlugin ask Announce(item), timeoutDuration)
    search ! Announce(item)
    println("announcing: " + item) 
    //println("response: " + res)
  }
  def search(search: ActorRef, text: String) {
    search.tell(Lookup(text), inbox.getRef)  
    
    def recvResult() { 
      receive() match {
        case e: TimeoutException => println("Cannot get result: " + e.getMessage)
        case SearchFinished(text) => {
          println("search finished " + text)
        }
        case res: Any => 
          println("search for " + text + " res: " + res)
          recvResult()
      }
    }
    recvResult()
  }
  
  
//  val first = new ContentItem(id, "long title", "good description", 1025)
//  val second = new ContentItem(Integer160.random.toString, "short title 2", "very good description", 1026)
//  
//  announce(searchPlugin, first)  
//  announce(searchPlugin, second)
//  
//  search(searchPlugin, "good")
//  search(searchPlugin, "long")
//  search(searchPlugin, "1025 bytes")
//  
//  println("-------------------------")
//  
//  search(searchPlugin2, "good")
  
 
  Thread.sleep(5 * 1000)

  println("node1 node list: " + node.storage.id + "@" + node.endpoint)
  node.storage.nodes.foreach(println)

  println("node2 node list: " + node2.storage.id + "@" + node2.endpoint)
  node2.storage.nodes.foreach(println)

  
  println("node3 node list: " + node3.storage.id + "@" + node3.endpoint)
  node2.storage.nodes.foreach(println)

  
  // fill the index
  
//  for (i <- 1 to 20) {
//    announce(new ContentItem(Integer160.random.toString, "title" + i, "description" + i, 1024 + i))   
//  }
//  
//  announce(first)  
//  announce(second)
  
  Thread.sleep(2 * 1000)

  
  node.dispose()
  node2.dispose()
  node3.dispose()

}