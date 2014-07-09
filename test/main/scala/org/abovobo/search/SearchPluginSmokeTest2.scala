package org.abovobo.search

import java.net.InetSocketAddress
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import akka.actor.Inbox
import akka.actor.Props
import akka.actor.ActorRef
import org.abovobo.dht.Controller
import org.abovobo.search.SearchPluginActor._
import org.abovobo.integer.Integer160
import akka.actor.ActorSystem
import scala.concurrent.duration._
import java.util.concurrent.TimeoutException
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import org.abovobo.dht.DhtNode
import scala.util.Random
import java.io.File
import org.abovobo.search.impl.LuceneContentIndex



object SearchPluginSmokeTest2 extends App {

  import collection.JavaConversions._

  val systemConfig = ConfigFactory.parseMap(Map(
      "akka.log-dead-letters" -> "true", 
      "akka.actor.debug.lifecycle" -> true,
      "akka.loglevel" -> "error",
      
    "akka.actor.debug.receive" -> true,
    "akka.actor.debug.unhandled" -> true))
    
  def localEndpoint(ordinal: Int) = new InetSocketAddress(InetAddress.getLocalHost, 20000 + ordinal)
  
  val system = ActorSystem("TestSystem", systemConfig)
  val timeoutDuration: FiniteDuration = 7.seconds
  implicit val timeout: Timeout = timeoutDuration
  
  def createNode(ordinal: Int, routers: List[InetSocketAddress] = List()): ActorRef = {
    system.actorOf(DhtNode.props(localEndpoint(ordinal), routers), "Node-" + ordinal.toString)
  }
  
  val searchIndexHome = {
    val h = new File(System.getProperty("user.home") + "/db/search")
    h.mkdirs
    h
  }
    
  def addSearchPlugin(node: ActorRef): ActorRef = {
    val info = Await.result(node ? DhtNode.Describe, timeoutDuration).asInstanceOf[DhtNode.NodeInfo]
    
    val name = node.path.name
    val home = new File(searchIndexHome, name).toPath()
      
    val searchPlugin = new SearchPlugin(
        home, 
        name,
        system, 
        info.controller, { () => info.self.id }, { () =>
      Await.result(node ? DhtNode.Describe, 5.seconds).asInstanceOf[DhtNode.NodeInfo].nodes
    })
    
    
    searchPlugin.indexManager ! IndexManagerActor.Clear 
    
    searchPlugin.search 
  }
  
  def printTable(node: ActorRef) {
    val info = Await.result(node ? DhtNode.Describe, timeoutDuration).asInstanceOf[DhtNode.NodeInfo]
    
    println("dht table for: " + info.self.id + "@" + info.self.address)
    println(info.nodes.size + " entries: " + info.nodes.mkString(", "))
  }
  
  
  val nodes = {
    val ep = new InetSocketAddress(InetAddress.getLocalHost, 30000 + 1)
    val node = DhtNode.createNode(system, ep, List(new InetSocketAddress(InetAddress.getLocalHost, 20000)))
    val search = addSearchPlugin(node)
    (ep, node, search) :: Nil
  }
     
  Thread.sleep(1 * 1000)
  
  println("--------- find node -------")
 
  val rnd = new Random
  
  rnd.shuffle(nodes).take(nodes.size) foreach { node =>
    node._2 ! Controller.FindNode(Integer160.random)
    Thread.sleep(1 * 1000)
  }

  println("--------- waiting -------")

  Thread.sleep(10 * 1000)  

  //for (i <- 1 to 15) 
  {
    
    println("Used Mem: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() * 1.0 / 1024 / 1024))
    System.gc()
    println("Used Mem: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() * 1.0 / 1024 / 1024))
    
    println("------------------------- table")  
    println
    nodes.foreach { n => printTable(n._2) }
    //Thread.sleep(60 * 1000)
  }
  
  println("--------- search test -------")
  
  
  def syncask(target: ActorRef, message: Any): Any = {
    Await.result(target ? message, 10.seconds)
  }

  val inbox = Inbox.create(system)  
  def search(text: String)(params: SearchParams)(implicit search: ActorRef): Set[ContentIndex.ContentRef] = {
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
    
    search.tell(Lookup(text, params), inbox.getRef)

    recvResult()
  }  
  
  
//  val first = ContentItem(Integer160.random.toString, "Cloud Atlas (2012)", 1025, new String(Files.readAllBytes(Paths.get("./test/test1.txt")), "UTF-8"))  
//  val second = ContentItem(Integer160.random.toString, "A Beautiful Mind (2001)", 1026, new String(Files.readAllBytes(Paths.get("./test/test2.txt")), "UTF-8"))
//  val shortOne = ContentItem(Integer160.random.toString, "short title 2", 100, "very short description")  
//  
  val announceGroup = rnd.shuffle(nodes).take(0)
//  
//  announceGroup.foreach { case (ep, node, search) =>
//    search ! SearchPlugin.Announce(first)
//    Thread.sleep(1000)
//    search ! SearchPlugin.Announce(second)
//    Thread.sleep(1000)    
//    search ! SearchPlugin.Announce(shortOne)
//  }
//  
//  Thread.sleep(5 * 1000)
//  
//  rnd.shuffle(nodes.filterNot(announceGroup.contains(_))).take(1).foreach { case (ep, node, sp) =>
//    def testSearch(text: String) = {
//      println(">>>>> Starting search for: " + text)
//      val res = search(text)(sp)
//      println(">>>>> Search finished. Search result for " + text + ": " + res)
//    }
//    
//    testSearch("Tom Hanks")
//    testSearch("can't find anything")
//    testSearch("\"1026 bytes\"")
//  }  
  
  val sp = nodes.head._3 
  
  var succeded = 0
  for (i <- 1 to 1000) {
    val res = search("Tom Hanks")(SearchParams(3, 3))(sp)
    if (res.isEmpty) {
      println("search: " + i + " FAILED")      
    } else {
      println("search: " + i + " SUCC")            
      succeded += 1
    }
    Thread.sleep(100)
  }
  println("search test done, succ count " + succeded)
  
  println("------------------------- waiting")
 
  Thread.sleep(5 * 1000)

  //for (i <- 1 to 5) 
  while(true)
  {
    println("------------------------- table")  
    nodes.foreach(n => printTable(n._2))
    Thread.sleep(5 * 60 * 1000)
  }
  
  println("------------------------- cleaning up")

  Thread.sleep(1000)
  nodes foreach { node => node._2 ! DhtNode.Stop } 
  Thread.sleep(3 * 1000)
  system.shutdown()
  system.awaitTermination()
}