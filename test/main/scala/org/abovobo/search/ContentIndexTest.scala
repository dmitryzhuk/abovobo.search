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
      index.clear()
    }
    
    "ContentIndex" when {
      "created" must {
        "be empty" in {
          index.clear()
          index.close()
          index = newIndex
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
          index.add(newItem())
          index.add(newItem())          
          index.add(item)
          index.lookup("title5").toList.exists(_.id == item.id) should be(true)
          val count = index.count
          index.remove(item.id)
          index.count should be(count - 1)          
          index.lookup("title5").toList.exists(_.id == item.id) should be(false)
        }
      }
      
      "clear() is called" must {
        "delete all items from index" in {
          val item = newItem("title5")
          index.add(newItem())
          index.add(newItem())          
          index.add(item)
          index.lookup("title5").toList.exists(_.id == item.id) should be(true)
          val count = index.count
          //index.remove(item.id)
          index.clear()
          index.count should be(0)          
          index.lookup("title5").toList.exists(_.id == item.id) should be(false)
        }
      }
      
    }
    
    def newItem(id: ContentIndex.CID = Integer160.random.toString, title: String = "title", description: String = "description", size: Long = 0): ContentItem = {
       ContentItem(id, title, size, description)
    }
}