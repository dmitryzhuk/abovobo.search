package org.abovobo.search.suite

object StartRouter extends App with SearchTestBase {
  val port = if (args.length > 0) args(0).toInt else super.routerPortBase
      
  override def routerPortBase = port
  
  override def debugLevel = "debug"
  
  val (routerEp, router) = createRouter()

  println("===== Starting router on localhost:" + routerEp.getPort + " =====")
  
  while (true) {
    Thread.sleep(Long.MaxValue)
  }
}