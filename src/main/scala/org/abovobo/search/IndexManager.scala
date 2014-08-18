package org.abovobo.search

import org.abovobo.search.ContentIndex.ContentRef
import org.abovobo.search.ContentIndex.ContentItem
import org.abovobo.search.IndexManagerRegistry.ContentItemStats
import IndexManager._

/**
 * This class is responsible for index content.
 * 
 * It stores indexed items' metadata (such as last hit timestamp, hits count etc) in a separate registry and 
 * put new items into index when there's enough space for this.
 * 
 * It is also responsible for index cleanup. For now index item lifetime looks like this:
 * - if there's enough space and no such item in the index, offered `offer(ContentItem)` item is indexed
 * - when index is full (items count >= maxItemsCount) no new items can be inserted
 * - when items count is in 'red zone' at some point `cleanup()` procedure starts
 * - cleanup() tries to remove items from 'red zone' and 'yellow' zone, leaving index in 'green zone'
 *   but by deleting obsolete items only (@see itemsLifetime)
 * - if no obsolete items are present during cleanup index might become full for some time
 * 
 * @constructor
 * @param ci actual index storage
 * @param reg index registry which accumulates metadata about indexed entries
 * @param maxItemsCount maximum amount of items that might be present in index at the same time
 * @param redZoneFactor 0 < factor < 1 which specifies when to start cleanup procedure
 * @param yellowZoneFactor 0 < factor < redZoneFactor which specifies how many items should be approached during cleanup
 * @param itemsLifetime minimum item lifetime in index
 */
class IndexManager(
    ci: ContentIndex, 
    reg: IndexManagerRegistry, 
    maxItemsCount: Int, 
    redZoneFactor: Double = DefaultRedZoneFactor , 
    yellowZoneFactor: Double = DefaultYellowZoneFactor ,
    itemsLifetime: Long = DefaultItemsLifetime ) {
  import IndexManager._
  
  def search(searchExpression: String): Set[ContentRef] = {
    val res = ci.lookup(searchExpression)
    
    val now = System.currentTimeMillis
    reg.get(res.map(_.id)).map { s => 
      s.copy(lastHit = now, hits = s.hits + 1) 
    } foreach(reg.update)
    
    res.toSet
  }
  
  def offer(item: ContentItem): OfferResult = {
    // first check do we have this item
    reg.get(item.id) match {
      case Some(stats) => {
        val updated = stats.copy(announces = stats.announces + 1, lastAnnounced = System.currentTimeMillis)
        reg.update(updated)
        AlreadyHave
      }
      case None => {
        // new item for us to consider
        if (reg.count < maxItemsCount) {
          // we always add new items if there's a space
          ci.add(item.decompress)
          reg.add(ContentItemStats(item.id, 0, 1))
          Accepted
        } else {
          RejectedNoSpace
        }
      }
    }
  }
  
  def cleanup() {
    // try to make items count < maxItemsCount * YellowZoneFactor
    val itemsCount = reg.count 
    val desiredCount = (maxItemsCount * yellowZoneFactor).toInt
    if (itemsCount > desiredCount) {
      val oldItemsDate = System.currentTimeMillis - itemsLifetime
      val oldItems = reg.lastAnnounced(oldItemsDate, itemsCount - desiredCount)
      oldItems.foreach { s => 
        ci.remove(s.id)
        reg.remove(s.id)
      }
    }
  }
  
  def cleanupNeeded: Boolean = reg.count > maxItemsCount * redZoneFactor
  
  def clear() = {
    ci.clear()
    reg.clear()
  }
  
  def close() {
    ci.close()
    reg.close()    
  }
}

object IndexManager {
  val DefaultRedZoneFactor = 0.9
  val DefaultYellowZoneFactor = 0.75
  
  val DefaultItemsLifetime = 3 * 24 * 60 * 60 * 1000 // 3 days
  
  sealed trait OfferResult
  
  case object Accepted extends OfferResult
  case object AlreadyHave extends OfferResult
  case object RejectedNoSpace extends OfferResult
  
  //case class RejectedBadContent extends OfferResult  
}