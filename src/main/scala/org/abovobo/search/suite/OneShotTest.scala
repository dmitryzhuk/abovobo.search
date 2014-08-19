package org.abovobo.search.suite

import java.net.InetSocketAddress
import java.net.InetAddress
import org.abovobo.dht.Requester
import org.abovobo.search.SearchPlugin
import org.abovobo.integer.Integer160

object OneShotTest extends App with SearchTestBase {
  val routerEp = if (args.length > 0) { 
    new InetSocketAddress(args(0).split(":")(0), args(0).split(":")(1).toInt)
  } else {
    new InetSocketAddress(InetAddress.getLocalHost(), routerPortBase)
  }
     
  
  override def homeDir = super.homeDir.resolve("test")
  override def portBase = 11000
  override def debugLevel = "error"
    
  println("Test using router " + routerEp)
  
  println("Doing announce")
  
  val announcerNode = null //DhtNode.createNode(homeDir.resolve("announcer"), system, localEndpoint(5555), List(routerEp))
  val announcerSearch = addSearchPlugin(announcerNode)
    
  Thread.sleep(1000)   
  
  //announcerNode ! new Requester.FindNode(Integer160.random)
  
  Thread.sleep(1000)
    
  announcerSearch ! SearchPlugin.FloodClearIndex
  
  Thread.sleep(5000)

  val item = randomItem
  
  announcerSearch ! SearchPlugin.Announce(item, SearchPlugin.AnnounceParams(4, 3))

  Thread.sleep(5000)  

  announcerSearch ! SearchPlugin.ClearIndex
 // announcerNode ! DhtNode.Stop
  
  Thread.sleep(1000)  
  
  println("Starting test nodes")  
  
  val testsCount = 100
  val testNodesCount = 10
  
  val requestorsNodes = spawnNodes(testNodesCount, List(routerEp)) { case (ep, node) =>
    println("created test node @ " + ep)
    (ep, node, addSearchPlugin(node))
  }  
  
  Thread.sleep(5000)
  
  println("=== Test Nodes Started ===")
  
  requestorsNodes.foreach { n =>
    printTable(n._2)
  }  

  val results = requestorsNodes.par.map { case (ep, node, search) =>
    val searcher = newSearcher
    var succeded = 0

    for (i <- 1 to testsCount) {
      val res = searcher(item.description, SearchPlugin.SearchParams(3, 3), search)
      if (res.isEmpty) {
        println("search: " + i + " FAILED")      
      } else {
        println("search: " + i + " SUCC")            
        succeded += 1
      }
    }
    (ep, succeded)    
  }
  
  println("Results: ")
  println("Tests count: " + testsCount)
    
  val resultsP = results.map { case (ep, succeded) =>
    val p = 1.0 * succeded / testsCount
    ep -> p
  }
  
  resultsP.foreach { case (ep, p) =>
    println("NodeInfo " + ep + ": p = " + p)
  }
  
  println("Totals:")
  
  println("mean p 1: " + resultsP.map(_._2).sum / resultsP.size)

  println("mean p 2: " + 1.0 * results.map(_._2).sum / (results.size * testsCount))
  
  println("Test complete. Shutting down.")
  
  announcerSearch ! SearchPlugin.FloodClearIndex

  Thread.sleep(1000)
  
  //announcerNode ! DhtNode.Stop
  //requestorsNodes.foreach { n => n._2 ! DhtNode.Stop }

  Thread.sleep(1000)
  
  //system.shutdown()
  //system.awaitTermination()
}