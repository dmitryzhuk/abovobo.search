package org.abovobo.search.suite

import java.net.InetSocketAddress
import org.abovobo.dht.DhtNode

object StartNetwork extends App with SearchTestBase {
  val routerEp = new InetSocketAddress(args(0).split(":")(0), args(0).split(":")(1).toInt)

  override def portBase = args(1).toInt

  val nodesCount = args(2).toInt
  
    
  println("===== Starting Network on local host, router: " + routerEp + ", nodes count: " + nodesCount + " =====")
  
  
  val nodes = spawnNodes(nodesCount, List(routerEp)) { (ep, node) =>
    println("Starting new node on " + ep)
    (ep, node, addSearchPlugin(node))
  }

  println("=================== NETWORK STARTED ===================")
  
  
  while(true) {
    println("\n\n")    
    println("=================== NETWORK TABLE ===================")    
    nodes.foreach { case (ep, node, search) =>
      printTable(node)
      Thread.sleep(100)
    }
    println("\n\n")
    Thread.sleep(5 * 60 * 1000)    
  }
}