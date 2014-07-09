package org.abovobo.search

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.abovobo.search.SearchPluginActor._
import org.abovobo.integer.Integer160
import org.abovobo.search.ContentIndex.ContentItem
import org.abovobo.search.ContentIndex._

class SearchMessageSerializationTest extends WordSpec with Matchers  {
  "SearchMessage" when {
    "serialized" must {
      "be parsable" in {
        createMessages foreach { msg =>
          val bin = SearchPluginActor.serializeMessage(msg) 
          val restored = SearchPluginActor.deserializeMessage(bin)
          println("testing " + msg)
          val restoredBin = SearchPluginActor.serializeMessage(restored) 
          SearchPluginActor.deserializeMessage(restoredBin) shouldEqual(msg)
          // we cannot compare binary representations 'cause items order is unspecified
        }
      }
    }
  }
  
  def newItem(id: ContentIndex.CID = Integer160.random.toString, title: String = "title", description: String = "description", size: Long = 0): ContentItem = {
     ContentItem(id, title, size, description)
  }
  
  def createMessages = {
    var list: List[SearchMessage] = Nil
    
    def id = Integer160.random.toString
    
    list ::= Announce(newItem(size = 123), AnnounceParams(2, 4) )
    list ::= Lookup("title", SearchParams(1, 1))
    list ::= FoundItems("title", Set.empty)
    list ::= FoundItems("title", Set(new SimpleContentRef(id, "title", 1234)))
    list ::= FoundItems("title", Set(
        new SimpleContentRef(id, "title", 1234),
        new SimpleContentRef(id, "title2", 1235)))
    list ::= SearchFinished("title")
    list ::= Error(666, "666")
        
    list.reverse
  } 
}