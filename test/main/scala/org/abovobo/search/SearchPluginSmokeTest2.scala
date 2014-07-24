package org.abovobo.search

import java.net.InetSocketAddress
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import akka.actor.Inbox
import akka.actor.Props
import akka.actor.ActorRef
import org.abovobo.dht.Controller
import org.abovobo.search.SearchPlugin._
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
import org.abovobo.search.ContentIndex.ContentItem
import java.nio.file.Paths
import java.nio.file.Files
import org.abovobo.search.suite.SearchTestBase
import java.util.concurrent.atomic.AtomicInteger



object SearchPluginSmokeTest2 extends App with SearchTestBase {
  override def debugLevel = "error"
    
  override def homeDir = Paths.get("~/db/smoke2-search-data")    

  val ep = new InetSocketAddress(InetAddress.getLocalHost, 30000 + 1)
  val node = DhtNode.createNode(homeDir.resolve("client"), system, ep, List(new InetSocketAddress(InetAddress.getLocalHost, routerPortBase)))
  val searchPlugin = addSearchPlugin(node)
     
  Thread.sleep(1 * 1000)
  
  println("--------- find node -------")
 
  node ! Controller.FindNode(Integer160.random)

  Thread.sleep(1 * 1000)
    
  println("--------- table -------")

  printTable(node)
  
  println("--------- search test -------")

  if (false) {
    val first = ContentItem(Integer160.random.toString, "Cloud Atlas (2012)", 1025, new String(Files.readAllBytes(Paths.get("./test/test1.txt")), "UTF-8"))  
    val second = ContentItem(Integer160.random.toString, "A Beautiful Mind (2001)", 1026, new String(Files.readAllBytes(Paths.get("./test/test2.txt")), "UTF-8"))
    val shortOne = ContentItem(Integer160.random.toString, "short title 2", 100, "very short description")  
    
    searchPlugin ! SearchPlugin.Announce(first)
    Thread.sleep(1000)
    searchPlugin ! SearchPlugin.Announce(second)
    Thread.sleep(1000)    
    searchPlugin ! SearchPlugin.Announce(shortOne)
    Thread.sleep(1 * 1000)    
  }
  
  searchPlugin ! SearchPlugin.ClearIndex
  
  val searchers = (1 to 10).map { _ => newSearcher }
  
  val succeded = new AtomicInteger(0)
  val failed = new AtomicInteger(0) 
  val testsCount = 2000
  val tests = (1 to testsCount).toList.toIterator
  searchers.par.foreach { search =>
    val tt = searchPlugin.synchronized {
      tests.take(testsCount / searchers.size).toList      
    }
    tt.foreach { i =>
      val res = search("Tom Hanks", SearchParams(3, 2), searchPlugin)
      if (res.isEmpty) {
        println("search: " + i + " FAILED")      
        failed.incrementAndGet
      } else {
        println("search: " + i + " SUCC")            
        succeded.incrementAndGet
      }
    }
  }
  println("search test done, total tests: " + testsCount + ", succ: " + succeded + ", failed: " + failed + ", p = " + 1.0 * succeded.get / testsCount)
  if (failed.get + succeded.get != testsCount) {
    throw new IllegalStateException("Invalid tests stats: failed: " + failed.get)
  }
  
  Thread.sleep(1000)

  println("------------------------- table")  
  
  printTable(node)
  
  println("------------------------- cleaning up")
  node ! DhtNode.Stop
  Thread.sleep(3 * 1000)
  system.shutdown()
  system.awaitTermination()
}