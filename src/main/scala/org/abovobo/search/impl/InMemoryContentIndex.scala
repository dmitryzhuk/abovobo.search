package org.abovobo.search.impl

import org.abovobo.search.ContentIndex._
import org.abovobo.search.ContentIndex

class InMemoryContentIndex extends ContentIndex {  
  @volatile
  private var items = new scala.collection.mutable.HashMap[CID, ContentItem]
  
  def contains(id: CID) = items.contains(id)
  
  def add(item: ContentItem) = items.put(item.id, item.decompress)
  
  def remove(id: CID) = items.remove(id)
  
  def lookup(searchString: String): Traversable[ContentRef] = 
    items.values filter { _.toString.contains(searchString) } map { ci => new SimpleContentRef(ci.id, ci.title, ci.size) }
  
  def list: Traversable[ContentRef] = items.values.map { ci => new SimpleContentRef(ci.id, ci.title, ci.size)  }
  
  def count = items.size
  
  def size = count * (ContentIndex.MaxDescriptionLength + ContentIndex.MaxTitleLenght + 20 + 8)
  
  def clear() = items.clear()
}