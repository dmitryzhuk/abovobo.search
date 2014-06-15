package org.abovobo.search

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.abovobo.integer.Integer160
import org.abovobo.search.ContentIndex._
import org.scalatest.BeforeAndAfterEach
import java.util.UUID

abstract class ContentIndexTest extends WordSpec with Matchers with BeforeAndAfterEach {
    var index: ContentIndex = null
    
    protected def newIndex(): ContentIndex 
    
    override def beforeEach() {
      index = newIndex()
    }
    
    "ContentIndex" when {
      "created" must {
        "be empty" in {
          index.count should be(0)
          index.lookup("*") should be('empty)
          index.clear()
        }
      }
      
      "new item added" must {
        "increment items count" in {
          index.count should be(0)
          index.add(newItem(title = "title123"))
          index.count should be(1)
          index.add(newItem(title = "title1234"))
          index.count should be(2)
        }
        "return this item in list" in {
          val item = newItem(title = "title123") 
          index.add(item)
          index.list.toList.exists(_.id == item.id) should be(true)
        }
        "return this item via content lookup" in {
          val item = newItem(title = "long title with several words title123") 
          index.add(item)
          index.lookup("several words").toList.exists(_.id == item.id) should be(true)
        }
      }
      
      "remove() is called" must {
        "delete item from index" in {
          val item = newItem("title5")
          index.add(item)
          index.lookup("title5").toList.exists(_.id == item.id) should be(true)
          index.remove(item.id)
          index.lookup("title5").toList.exists(_.id == item.id) should be(false)
        }
      }
    }
    
    def newItem(id: ContentIndex.CID = Integer160.random.toString, title: String = "title", description: String = "description", size: Long = 0): ContentItem = {
       new ContentItem(id, title, description, size)
    }
}