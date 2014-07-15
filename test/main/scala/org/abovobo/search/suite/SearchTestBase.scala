package org.abovobo.search.suite

import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

import org.abovobo.dht.DhtNode
import org.abovobo.search.ContentIndex
import org.abovobo.search.SearchPlugin
import org.abovobo.search.SearchPlugin.FoundItems
import org.abovobo.search.SearchPlugin.Lookup
import org.abovobo.search.SearchPlugin.SearchFinished
import org.abovobo.search.SearchPlugin.SearchParams

import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Inbox
import akka.pattern.ask
import akka.util.Timeout
import akka.util.Timeout.durationToTimeout


trait SearchTestBase {
  import collection.JavaConversions._
  import scala.concurrent.duration._
  
  val timeoutDuration: FiniteDuration = 5.seconds
  implicit val timeout: Timeout = timeoutDuration

  def systemConfig = {
    ConfigFactory.parseMap(Map(
      "akka.log-dead-letters" -> "true", 
      "akka.actor.debug.lifecycle" -> true,
      "akka.loglevel" -> debugLevel,  
      "akka.actor.debug.receive" -> true,
      "akka.actor.debug.unhandled" -> true))    
  }
  
  def debugLevel = "info"
  
  def createSystem(name: String) = ActorSystem(name, systemConfig)  
  
  def portBase = 20000
  
  def routerPortBase = 10000
  
  def createRouter(ordinal: Int = 0) = {
    val routerEp = new InetSocketAddress(InetAddress.getLocalHost, routerPortBase + ordinal)
    val router = DhtNode.createNode(system, routerEp)
    (routerEp, router)
  }
  
  def localEndpoint(ordinal: Int) = new InetSocketAddress(InetAddress.getLocalHost, portBase + ordinal)  
  
  def printTable(node: ActorRef) {
    val info = Await.result(node ? DhtNode.Describe, timeoutDuration).asInstanceOf[DhtNode.NodeInfo]
    
    println("Dht table for: " + info.self.id + "@" + info.self.address)
    println(info.nodes.size + " entries: " + info.nodes.mkString(", "))
  }  
  
  def searchIndexHome = new File(System.getProperty("user.home") + "/db/search")

  def addSearchPlugin(node: ActorRef): ActorRef = {
    val info = Await.result(node ? DhtNode.Describe, timeoutDuration).asInstanceOf[DhtNode.NodeInfo]
    
    val name = node.path.name
    val home = new File(searchIndexHome, name).toPath()
      
    val search = SearchPlugin(
        home, 
        name,
        system, 
        info.controller, { () => info.self.id }, { () =>
      Await.result(node ? DhtNode.Describe, timeoutDuration).asInstanceOf[DhtNode.NodeInfo].nodes
    })

    search
  }
    
  def newSearcher = {
    val inbox = Inbox.create(system)  
    def search(text: String, params: SearchParams, searchPlugin: ActorRef): Set[ContentIndex.ContentRef] = {
      var result = Set.empty[ContentIndex.ContentRef]
        
      def receive() = {
        try {
          inbox.receive(timeoutDuration)
        } catch {
          case e: Exception => e
        }    
      }
      
      def recvResult(): Set[ContentIndex.ContentRef] =  { 
        receive() match {
          case e: Exception => println("Cannot get result: " + e.getMessage); result
          case SearchFinished(text) => result
          case FoundItems(text, items) => 
            result ++= items.toSet
            recvResult()
        }
      }
      
      searchPlugin.tell(Lookup(text, params), inbox.getRef)
  
      recvResult()
    }
    search _    
  }

  
  lazy val system = createSystem("TestSystem")
  val rnd = new Random
}