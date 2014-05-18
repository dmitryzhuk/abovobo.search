package org.abovobo.search.impl

import org.abovobo.search.ContentIndex._
import org.abovobo.search.ContentIndex

class InMemoryContentIndex extends ContentIndex {
  
  private class SimpleContentItem(id: CID, title: String, description: String, size: Long, 
      val ts: Long = System.currentTimeMillis, var hits: Long = 0) 
      extends ContentItem(id, title, description, size) {
    
    def this(ci: ContentItem) = this(ci.id, ci.title, ci.description, ci.size)
  }
  
  @volatile
  private var items = new scala.collection.mutable.HashMap[CID, SimpleContentItem]
  
  def contains(id: CID) = items.contains(id)
  
  def add(item: ContentItem) = items.put(item.id, new SimpleContentItem(item))
  
  def remove(id: CID) = items.remove(id)
  
  def lookup(searchString: String): Traversable[ContentRef] = null
    //items.values filter { _.toString.contains(searchString) } map { ci => new ContentRef(ci.id, ci.title. 0, 0) }
  
  def list: Traversable[ContentRef] = null// items.values.map { ci => new ContentRef(ci.id, ci.title. ci.ts, ci.hits)  }
  
  def count = items.size
  
  def size = count * (ContentIndex.MaxDescriptionLength + ContentIndex.MaxTitleLenght + 20 + 8)
  
  def clear() = items.clear()
}