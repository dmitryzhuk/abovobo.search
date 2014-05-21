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



object SearchPluginSmokeTest extends App {
  val systemConfig = ConfigFactory.parseMap(Map(
      "akka.log-dead-letters" -> "true", 
      "akka.actor.debug.lifecycle" -> true,
      "akka.loglevel" -> "debug",
      
    "akka.actor.debug.receive" -> true,
    "akka.actor.debug.unhandled" -> true))
    
    
  case class NodeSystem(endpoint: InetSocketAddress, table: ActorRef, agent: ActorRef, controller: ActorRef, system: ActorSystem, storage: H2Storage) extends Disposable {
    def dispose() {
        system.shutdown()
        system.awaitTermination()
        storage.close()     
    }
  }
  
  def localEndpoint(ordinal: Int) = new InetSocketAddress(InetAddress.getLocalHost, 20000 + ordinal)
    
  def createNode(ordinal: Int, routers: List[InetSocketAddress] = List()): NodeSystem = {
      val h2 = H2Storage.open("~/db/dht-" + ordinal, true) 

    val storage: Storage = h2
    val reader: Reader = h2
    val writer: Writer = h2
      
    val system = ActorSystem("TestSystem-" + ordinal, systemConfig)
    
    val table = system.actorOf(Table.props(reader, writer), "table")    
    val agent = system.actorOf(Props(classOf[Agent], localEndpoint(ordinal), 10 seconds), "agent")
    val controller = system.actorOf(Controller.props(routers, reader, writer), "controller")
  
    NodeSystem(localEndpoint(ordinal), table, agent, controller, system, h2)
  }
  
  val node = createNode(1)
  
  //val routerEndpoints = List(localEndpoint(1))
  val indexManager = new IndexManager(10, new InMemoryContentIndex(), new IndexManagerRegistry("jdbc:h2:~/db/search"))
  val searchPlugin = node.system.actorOf(Props(classOf[SearchPlugin], Plugin.SearchPluginId, node.controller, node.storage, indexManager), "search")
  
  val id = Integer160.random.toString

  implicit val inbox = Inbox.create(node.system)

  def receive() = {
    try {
      inbox.receive(15 seconds)
    } catch {
      case e: TimeoutException => "<timeout...>"
    }    
  }
  
  def announce(item: ContentItem) {
    searchPlugin.tell(Announce(item), inbox.getRef)
    println("announced: " + item) 
    println("response: " + receive())
  }
  def search(text: String) {
    searchPlugin.tell(Lookup(text), inbox.getRef)  
    val res = receive()
    println("search for " + text + " res: \n" + res)    
  }
  
  indexManager.clear()
  
  val first = new ContentItem(id, "long title", "good description", 1025)
  val second = new ContentItem(Integer160.random.toString, "short title 2", "very good description", 1026)
  
  announce(first)  
  announce(second)
  
  search("good")
  search("long")
  search("1025 bytes")
  
  
  // fill the index
  
  for (i <- 1 to 20) {
    announce(new ContentItem(Integer160.random.toString, "title" + i, "description" + i, 1024 + i))   
  }
  
  announce(first)  
  announce(second)

  
  node.dispose()

}