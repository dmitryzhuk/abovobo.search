package org.abovobo.search

import org.abovobo.search.ContentIndex._
import org.abovobo.integer.Integer160
import org.abovobo.search.IndexManagerRegistry.ContentItemStats

object IndexManagerRegistry {  
  case class ContentItemStats(
      id: String, 
      hits: Int = 0, 
      announces: Int = 0, 
      added: Long = System.currentTimeMillis, 
      lastAnnounced: Long = System.currentTimeMillis, 
      lastHit: Long = System.currentTimeMillis)
}

trait IndexManagerRegistry { 
  def get(id: String): Option[ContentItemStats]

  def get(ids: Traversable[String]): Traversable[ContentItemStats]

  def add(s: ContentItemStats)
  
  def add(ss: Iterable[ContentItemStats])
  
  def update(s: ContentItemStats): Boolean
  
  def remove(id: String): Option[ContentItemStats]
  
  def lastAnnounced(olderThan: Long, top: Int): Iterable[ContentItemStats]
  
  def stats: Iterable[ContentItemStats]
  
  def count: Int
  
  def clear()
  
  def close()
}