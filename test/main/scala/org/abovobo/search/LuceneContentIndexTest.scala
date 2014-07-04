package org.abovobo.search

import org.abovobo.search.impl.LuceneContentIndex
import org.scalatest.BeforeAndAfterAll
import java.io.File
import java.nio.file.Files
import java.util.UUID

class LuceneContentIndexTest extends ContentIndexTest with BeforeAndAfterAll {
  val tempDir = new File("./tmp/index")
  tempDir.mkdirs
  
  override def afterEach() {
    index.clear()
    index.close()
  }
  
  override def afterAll() {
    //Directory
    //Files.delete(tempDir.toPath)
  }
  
  def newIndex = new LuceneContentIndex(tempDir.toPath)
  
  "LuceneIndex" when {
      "field is specified" must {
        "search by this field" in {
          val item1 = newItem(title = "title swimming", description = "description swimming shooting")
          val item2 = newItem(title = "title swimming shooting", description = "description swimming")
          index.add(item1)
          index.add(item2)
          
          index.lookup("shoot").toList.size should be(2)
          index.lookup("title:shoot").toList.size should be(1)
          index.lookup("description:shoot").toList.size should be(1)

          index.lookup("title:shoot").toSet.contains(item2) should be(true)
          index.lookup("description:shoot").toSet.contains(item1) should be(true)
        }
      }
      "items are added" must {
        "grow in size" in {
          afterEach()
          
          index = new LuceneContentIndex(tempDir.toPath)
          
          val n = 1000
          
          for (i <- 1 to n) {
            val item = newItem(title = "title title_" + UUID.randomUUID.toString, description = "description description_" + UUID.randomUUID.toString)
            index.add(item)
          }
          index.close()
          index = new LuceneContentIndex(tempDir.toPath)

          val oldSize = index.size
          
          for (i <- 1 to n) {
            val item = newItem(title = "title title_" + UUID.randomUUID.toString, description = "description description_" + UUID.randomUUID.toString)
            index.add(item)
          }
          index.close()          
          val newSize = index.size
          newSize should be > oldSize
          
          // for cleanup:
          index = new LuceneContentIndex(tempDir.toPath)
        }
      }
  }
  
}