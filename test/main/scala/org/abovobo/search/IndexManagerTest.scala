package org.abovobo.search

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.abovobo.search.impl.LuceneContentIndex
import java.io.File
import org.abovobo.integer.Integer160
import org.abovobo.search.ContentIndex.ContentItem
import org.abovobo.search.IndexManager._


/**
 * IndexManager - ContentIndex - IndexManagerRegistry integration test
 */
class IndexManagerTest extends WordSpec with Matchers {
  val tempDir = new File("./tmp/index")
  tempDir.mkdirs

  def newIndex = new LuceneContentIndex(tempDir.toPath)
  
  def newRegistry = new IndexManagerRegistry("jdbc:h2:./tmp/index/reg")
    
  "IndexManager" when {
    "instantiated be empty" in {
      val im = new IndexManager(newIndex, newRegistry, 100)
      im.clear()
      im.cleanupNeeded should be(false)
      im.close()
    }
    "is empty must accept new items" in {
      val im = new IndexManager(newIndex, newRegistry, 100)
      im.clear()
      im.offer(newItem()) should be(Accepted)
      im.offer(newItem()) should be(Accepted)
      im.offer(newItem()) should be(Accepted)
      im.close()
    }
    "new item is added" must {
      "return it with search" in {
        val im = new IndexManager(newIndex, newRegistry, 100)
        im.clear()  
        val item = newItem(title = "title777")
        im.offer(item) should be(Accepted)
        im.search("title777").toList.exists(_.id == item.id) should be(true)
        im.search(item.id).toList.exists(_.id == item.id) should be(true)        
      }
    }
    "full" must {
      "reject new items" in {
        val im = new IndexManager(newIndex, newRegistry, 5)
        im.clear()
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(RejectedNoSpace)        
        im.close()
      }
      "after clear be empty again" in {
        val im = new IndexManager(newIndex, newRegistry, 5)
        im.clear()
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(RejectedNoSpace)
        im.clear()
        im.offer(newItem()) should be(Accepted)
        im.close()
      }
      "report that cleanup is needed" in {
        val im = new IndexManager(newIndex, newRegistry, 3)
        im.clear()
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.cleanupNeeded should be(true)
        im.close()
      }
      "perform cleanup correctly" in {        
        val index = newIndex
        val reg = newRegistry
        val max = 20
        val im = new IndexManager(index, reg, max, 0.8, 0.6, 1000)
        im.clear()

        val item2 = newItem(title = "item2")        
        val item1 = newItem(title = "item1")
        
        im.offer(item1) should be(Accepted)
        
        for (i <- 1 to max - 2) {
          im.offer(newItem()) should be(Accepted)
          Thread.sleep(10)
        }
        im.cleanupNeeded should be(true)
        im.offer(item2) should be(Accepted)
        
        im.offer(newItem()) should be(RejectedNoSpace)
        im.cleanup() // no items deleted here, `cause they are not expired yet
        im.offer(newItem()) should be(RejectedNoSpace)
        
        im.search(item1.id).toList.exists(_.id == item1.id) should be(true)
        im.search(item2.id).toList.exists(_.id == item2.id) should be(true)
         
        Thread.sleep(1100)
        
        im.cleanup()
        
        reg.count should equal (index.count)
        index.count should be <= ((max * 0.6).toInt)
        
        // make sure older items are deleted and newer are here
        
        im.search(item1.id).toList.exists(_.id == item1.id) should be(false)
        im.search(item2.id).toList.exists(_.id == item2.id) should be(true)
        
        im.offer(item1) should be(Accepted)
        im.offer(item2) should be(AlreadyHave)
      }
    }
    "duplicate item offered reject item with AlreadyHave" in {
        val im = new IndexManager(newIndex, newRegistry, 5)
        im.clear()
        val item = newItem()
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)
        im.offer(newItem()) should be(Accepted)

        im.offer(item) should be(Accepted)
        im.offer(item) should be(AlreadyHave)
        
        im.offer(newItem()) should be(Accepted)

        im.offer(item) should be(AlreadyHave)
        
        im.clear()

        im.offer(item) should be(Accepted)
        
        im.close()      
    }
  }

  def newItem(id: ContentIndex.CID = Integer160.random.toString, title: String = "title", description: String = "description", size: Long = 0): ContentItem = {
    ContentItem(id, title, size, description)
  }  
}