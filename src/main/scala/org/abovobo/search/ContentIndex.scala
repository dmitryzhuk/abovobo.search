package org.abovobo.search

import akka.actor.Actor
import org.abovobo.integer.Integer160

import ContentIndex._

trait ContentIndex {
  def add(item: ContentItem)
  
  def remove(id: CID)

  def list: Traversable[ContentRef]

  def contains(id: CID): Boolean

  def lookup(searchExpression: String): Traversable[ContentRef]
      
  /** delete all items from index */
  def clear()

  /** @return items count */
  def count: Int
  
  /** @return approximate index size in bytes */
  def size: Long
}

object ContentIndex {
  type CID = String
  
  val MaxTitleLenght = 256
  val MaxDescriptionLength = 512
  
  class ContentItem(val id: CID, val title: String, val description: String, val size: Long) {
    
    if (title.length() > MaxTitleLenght) throw new IllegalArgumentException("title is too long")
    if (description.length() > MaxDescriptionLength) throw new IllegalArgumentException("description is too long")
    
    override def toString = id.toString + " : " + title + " : " + description + " : " + size + " bytes"
  }
 
  class ContentRef(val id: CID, val title: String, val ts: Long, val hitsCount: Long)
}
