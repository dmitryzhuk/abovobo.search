package org.abovobo.search

import org.abovobo.search.ContentIndex.ContentRef
import org.abovobo.search.ContentIndex.ContentItem
import org.abovobo.search.IndexManagerRegistry.ContentItemStats

class IndexManager(val maxItemsCount: Int, ci: ContentIndex, reg: IndexManagerRegistry) {
  import IndexManager._
  
  def search(searchExpression: String): Traversable[ContentRef] = {
    val res = ci.lookup(searchExpression)
    
    val now = System.currentTimeMillis
    reg.get(res.map(_.id)).map { s => 
      s.copy(lastHit = now, hits = s.hits + 1) 
    } foreach(reg.update)
    
    res
  }
  
  def offer(item: ContentItem): OfferResponse = {
    // first check do we have this item
    reg.get(item.id) match {
      case Some(stats) => {
        val updated = stats.copy(announces = stats.announces + 1, lastAnnounced = System.currentTimeMillis)
        reg.update(updated)
        AlreadyHave()
      }
      case None => {
        // new item for us to consider
        if (reg.count < maxItemsCount) {
          // we always add new items if there's a space
          ci.add(item)
          reg.add(ContentItemStats(item.id, 0, 1))
          Accepted()
        } else {
          RejectedNoSpace()
        }
      }
    }
  }
  
  def cleanup() {
    // try to make items count < maxItemsCount * YellowZoneFactor
    val itemsCount = reg.count 
    val desiredCount = (maxItemsCount * YellowZoneFactor).toInt
    if (itemsCount > desiredCount) {
      //val oldItems = reg.sorted({ }, Some(10))
      val oldItemsDate = System.currentTimeMillis - ItemsLifetime
      val oldItems = reg.lastAnnounced(oldItemsDate, desiredCount - itemsCount)
      oldItems.foreach { s => reg.remove(s.id) }
    }
  }
  
  def cleanupNeeded: Boolean = reg.count > maxItemsCount * RedZoneFactor
  
  def clear() = {
    ci.clear()
    reg.clear()
  }
  
  // TODO: call cleanup/rotation procedure
}

object IndexManager {
  val RedZoneFactor = 0.9
  val YellowZoneFactor = 0.75
  
  val ItemsLifetime = 3 * 24 * 60 * 60 * 1000 // days
  
  sealed trait OfferResponse
  
  case class Accepted extends OfferResponse
  case class AlreadyHave extends OfferResponse
  case class RejectedNoSpace extends OfferResponse
  
  //case class RejectedBadContent extends OfferResponse  
}