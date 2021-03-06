package org.abovobo.search

import java.net.InetSocketAddress
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import akka.actor.Inbox
import akka.actor.Props
import akka.actor.ActorRef
import org.abovobo.dht.Requester
import org.abovobo.search.SearchPlugin._
import org.abovobo.search.ContentIndex.ContentItem
import org.abovobo.integer.Integer160
import akka.actor.ActorSystem
import scala.concurrent.duration._
import java.util.concurrent.TimeoutException
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.util.Random
import java.io.File
import org.abovobo.search.impl.LuceneContentIndex
import java.nio.file.Files
import java.nio.file.Paths
import org.abovobo.search.suite.SearchTestBase



object SearchPluginSmokeTest extends App with SearchTestBase {

  override def homeDir = Paths.get("~/db/smoke-search-data")

  import collection.JavaConversions._
  
  val (routerEp, router) = createRouter()
  
  val nodes = spawnNodes(50, List(routerEp)) { (ep, node) =>
    Thread.sleep(750)   
    println("Started new node on " + ep)
    (ep, node, addSearchPlugin(node))
  }
  
  Thread.sleep(5 * 1000)
  
  println("--------- find node -------")
 
  rnd.shuffle(nodes).take(nodes.size / 10) foreach { node =>
    node._2 ! Requester.FindNode(Integer160.random)
    Thread.sleep(1 * 1000)
  }

  println("--------- waiting -------")

  Thread.sleep(10 * 1000)  

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
  
  val first = ContentItem(Integer160.random.toString, "Cloud Atlas (2012)", 1025, new String(Files.readAllBytes(Paths.get("./test/test1.txt")), "UTF-8"))  
  val second = ContentItem(Integer160.random.toString, "A Beautiful Mind (2001)", 1026, new String(Files.readAllBytes(Paths.get("./test/test2.txt")), "UTF-8"))
  val shortOne = ContentItem(Integer160.random.toString, "short title 2", 100, "very short description")
  
  
  def syncask(target: ActorRef, message: Any): Any = {
    Await.result(target ? message, 10.seconds)
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
  
  val announceGroup = rnd.shuffle(nodes).take(1)

  /*
  announceGroup.foreach { case (ep, node, search) =>
    search ! SearchPlugin.Announce(first)
    Thread.sleep(1000)
    search ! SearchPlugin.Announce(second)
    Thread.sleep(1000)    
    search ! SearchPlugin.Announce(shortOne)
  }
  */
  
  Thread.sleep(5 * 1000)

  /*
  rnd.shuffle(nodes.filterNot(announceGroup.contains(_))).take(1).foreach { case (ep, node, sp) =>
    def testSearch(text: String) = {
      println(">>>>> Starting search for: " + text)
      val res = search(text)(sp)
      println(">>>>> Search finished. Search result for " + text + ": " + res)
    }
    
    testSearch("Tom Hanks")
    testSearch("can't find anything")
    testSearch("\"1026 bytes\"")
  }
  */
  
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
  //nodes foreach { node => node._2 ! DhtNode.Stop }
  Thread.sleep(3 * 1000)
  system.shutdown()
  system.awaitTermination()
}