package org.abovobo.search.suite

import java.net.InetSocketAddress
import org.abovobo.dht.DhtNode

object StartNetwork extends App with SearchTestBase {
  val routerEp = new InetSocketAddress(args(0).split(":")(0), args(0).split(":")(1).toInt)
  val nodesCount = args(2).toInt
  
  override def portBase = args(1).toInt
  
    
  println("===== Starting Network on local host, router: " + routerEp + ", nodes count: " + nodesCount + " =====")
  
  
  val nodes = spawnNodes(nodesCount, List(routerEp)) { (ep, node) =>
    Thread.sleep(750)   
    println("Starting new node on " + ep)
    (ep, node, addSearchPlugin(node))
  }

  println("=================== NETWORK STARTED ===================")
  
  
  while(true) {
    Thread.sleep(5 * 60 * 1000)
    println("=================== NETWORK STARTED ===================")    
    nodes.foreach { case (ep, node, search) =>
      printTable(node)
    }
    println("\n\n")
  }
}