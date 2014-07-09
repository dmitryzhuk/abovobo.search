package org.abovobo.search

import akka.actor.ActorSystem
import org.abovobo.dht.Node
import org.abovobo.integer.Integer160
import akka.actor.ActorRefFactory
import akka.actor.ActorRef
import java.nio.file.Path
import org.abovobo.search.impl.LuceneContentIndex
import SearchPlugin._
import org.abovobo.dht.PID
import akka.actor.Props
import org.abovobo.dht.Controller

class SearchPlugin(
    val homeDir: Path,
    val name: String, 
    val parent: ActorRefFactory, 
    val messageChannel: ActorRef,
    selfId: () => Integer160, 
    dhtNodes: () => Traversable[Node],
    val maxItemsCount: Int = SearchPlugin.DefaultMaxItemsCount) {
  
  val (search, indexManager) = {
    val indexDir = homeDir.resolve("index")
    indexDir.toFile.mkdirs
    
    val index = new LuceneContentIndex(indexDir)
    
    val indexManager = new IndexManager(index, new IndexManagerRegistry("jdbc:h2:" + homeDir.resolve(name)), maxItemsCount)

    val indexManagerActor = parent.actorOf(Props(classOf[IndexManagerActor], indexManager), name + "-im")
    
    val searchPluginActor = parent.actorOf(SearchPluginActor.props(selfId, messageChannel, indexManagerActor, dhtNodes))
    
    // subscribe for messages
    messageChannel ! Controller.PutPlugin(PluginId, searchPluginActor)
    
    (searchPluginActor, indexManagerActor)
  }
  
  val pluginId = SearchPlugin.PluginId
}

object SearchPlugin {
  val PluginId = new PID(1)
  val DefaultMaxItemsCount = 10000
}