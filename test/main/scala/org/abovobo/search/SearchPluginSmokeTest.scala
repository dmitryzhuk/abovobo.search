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
import org.abovobo.dht.DhtNode
import scala.util.Random
import java.io.File
import org.abovobo.search.impl.LuceneContentIndex



object SearchPluginSmokeTest extends App {
  val systemConfig = ConfigFactory.parseMap(Map(
      "akka.log-dead-letters" -> "true", 
      "akka.actor.debug.lifecycle" -> true,
      "akka.loglevel" -> "debug",
      
    "akka.actor.debug.receive" -> true,
    "akka.actor.debug.unhandled" -> true))
    
  def localEndpoint(ordinal: Int) = new InetSocketAddress(InetAddress.getLocalHost, 20000 + ordinal)
  
  val system = ActorSystem("TestSystem", systemConfig)
  val timeoutDuration: FiniteDuration = 7 seconds
  implicit val timeout: Timeout = timeoutDuration
  
  def createNode(ordinal: Int, routers: List[InetSocketAddress] = List()): ActorRef = {
    system.actorOf(DhtNode.props(localEndpoint(ordinal), routers), "Node-" + ordinal.toString)
  }
  
  val luceneIndexHome = {
    val h = new File(System.getProperty("user.home") + "/db/lucene")
    h.mkdirs
    h
  }
    
  def addSearchPlugin(node: ActorRef): ActorRef = {
    
    val info = Await.result(node ? DhtNode.Describe, timeoutDuration).asInstanceOf[DhtNode.NodeInfo]
    
    def getContentIndex = {
      val dir = new File(luceneIndexHome, node.path.name)
      dir.mkdirs
      new LuceneContentIndex(dir.toPath)
    }
    
    val indexManager = system.actorOf(Props(classOf[IndexManagerActor], 
        new IndexManager(10, getContentIndex, new IndexManagerRegistry("jdbc:h2:~/db/search-" + node.path.name))), node.path.name + "-indexManager")
        
    val searchPlugin = system.actorOf(SearchPlugin.props(info.self.id, info.controller, indexManager, { () =>
      Await.result(node ? DhtNode.Describe, 5 seconds).asInstanceOf[DhtNode.NodeInfo].nodes
    }), node.path.name + "-search")

    indexManager ! IndexManagerActor.Clear 
    
    info.controller ! PutPlugin(Plugin.SearchPluginId, searchPlugin)
    
    searchPlugin
  }
  
  def printTable(node: ActorRef) {
    val info = Await.result(node ? DhtNode.Describe, timeoutDuration).asInstanceOf[DhtNode.NodeInfo]
    
    println("dht table for: " + info.self.id + "@" + info.self.address)
    info.nodes.foreach { entry => println("\t" + entry)}
  }
  
  val epnodes = DhtNode.spawnNodes(system, 20000, 12) 
  val nodes = epnodes.map { _._2 }
  
  // omit router node from search process
  val searchPlugins = nodes.tail map { node => node -> addSearchPlugin(node) }

  Thread.sleep(5 * 1000)
  
  println("--------- waiting -------")
 
//  for (i <- 1 to 5) {
//    println("--------- find node -------")
//
//    nodes.foreach { n => 
//      n ! Controller.FindNode(Integer160.random)
//    }
//    Thread.sleep(10 * 1000)
//  }
  
  Thread.sleep(10 * 1000)

  //for (i <- 1 to 15) 
  {
    
    println("Used Mem: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() * 1.0 / 1024 / 1024))
    System.gc()
    println("Used Mem: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() * 1.0 / 1024 / 1024))
    
    println("------------------------- table")  
    println
    nodes.foreach(printTable)
    Thread.sleep(60 * 1000)
  }
  
  println("--------- search test -------")
  
  val first = new ContentItem(Integer160.random.toString, "long title", "good description", 1025)
  val second = new ContentItem(Integer160.random.toString, "short title 2", "very good description", 1026)
  
  val rnd = new Random
  
  def syncask(target: ActorRef, message: Any): Any = {
    Await.result(target ? message, 10 seconds)
  }
  
  def search(text: String)(implicit search: ActorRef): Set[ContentIndex.ContentRef] = {
    val inbox = Inbox.create(system)
    var result = Set.empty[ContentIndex.ContentRef]
      
    def receive() = {
      try {
        inbox.receive(timeoutDuration)
      } catch {
        case e: TimeoutException => e
      }    
    }
    
    def recvResult(): Set[ContentIndex.ContentRef] =  { 
      receive() match {
        case e: TimeoutException => println("Cannot get result: " + e.getMessage); result
        case SearchFinished(text) => result
        case FoundItems(text, items) => 
          result ++= items.toSet
          recvResult()
      }
    }
    
    search.tell(Lookup(text), inbox.getRef)  

    recvResult()
  }  
  
  val announceGroup = rnd.shuffle(searchPlugins).take(1)
  
  announceGroup.foreach { case (node, search) =>
    search ! SearchPlugin.Announce(first)
    search ! SearchPlugin.Announce(second)
  }
  
  Thread.sleep(3 * 1000)
  
  rnd.shuffle(searchPlugins.filterNot(announceGroup.contains(_))).take(1).foreach { case (node, sp) =>
    def testSearch(text: String) = {
      println(">>>>> Starting search for: " + text)
      val res = search(text)(sp)
      println(">>>>> Search finished. Search result for " + text + ": " + res)
    }
    
    testSearch("good")
    testSearch("long")
    testSearch("1025 bytes")
  }  
  
  println("------------------------- waiting")
 
  Thread.sleep(5 * 1000)

  //for (i <- 1 to 15) 
  {
    println("------------------------- table")  
    nodes.foreach(printTable)
  }
  
  println("------------------------- cleaning up")

  Thread.sleep(1000)
  nodes foreach { node => node ! DhtNode.Stop } 
  Thread.sleep(3 * 1000)
  system.shutdown()
  system.awaitTermination()
}