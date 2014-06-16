package org.abovobo.search

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.abovobo.search.ContentIndex.ContentItem
import org.abovobo.integer.Integer160

class ContentItemTest extends WordSpec with Matchers  {
  "ContentItem" when {
    "created" must {
      "have member initialized" in {
        val ci = newItem(title = "", description = "", size = 0)
        ci.description.length should be(0)
        ci.title.length should be(0)
        val id = Integer160.random.toString
        val ci2 = ContentItem(id, "title 123", 1234, "description 1234")
        ci2.id shouldEqual id
        ci2.title shouldEqual "title 123"
        ci2.description shouldEqual "description 1234"
        ci2.size shouldEqual 1234
      }
    }
    
  }
  
  def newItem(id: ContentIndex.CID = Integer160.random.toString, title: String = "title", description: String = "description", size: Long = 0): ContentItem = {
     ContentItem(id, title, size, description)
  }  
}