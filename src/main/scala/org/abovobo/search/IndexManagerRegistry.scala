package org.abovobo.search

import org.abovobo.search.ContentIndex._
import scala.slick.driver.H2Driver.simple._
import java.sql.Date
import org.abovobo.integer.Integer160
import org.abovobo.search.IndexManagerRegistry.CIStats
import org.abovobo.search.IndexManagerRegistry.ContentItemStats
import scala.slick.jdbc.meta.MTable
import scala.slick.jdbc.StaticQuery
import scala.slick.util.CloseableIterator
import java.io.Closeable


object IndexManagerRegistry extends App {
  
  case class ContentItemStats(id: String, hits: Int = 0, announces: Int = 0, added: Long = System.currentTimeMillis, lastAnnounced: Long = System.currentTimeMillis, lastHit: Long = System.currentTimeMillis)

  object CIStats {
    val TableName = "ContentItemStats"
  }
  
  class CIStats(tag: Tag) extends Table[ContentItemStats](tag, None, CIStats.TableName) {      
    def id = column[String]("ID", O.PrimaryKey)

    def hits = column[Int]("hits", O.Default(0))
    def announces = column[Int]("announces", O.Default(0))
    
    def added = column[Long]("added")
    def lastAnnounced = column[Long]("lastAnnounced") 
    
    def lastHit = column[Long]("lastHit")
    
    def * = (id, hits, announces, added, lastAnnounced, lastHit) <> (ContentItemStats.tupled, ContentItemStats.unapply)
  }
}

class IndexManagerRegistry(dbUrl: String) extends Closeable {
  private val db = Database.forURL(dbUrl, driver = "org.h2.Driver")
  protected val stats: TableQuery[CIStats] = TableQuery[CIStats]
  
  protected implicit val session = db.createSession
    
  def get(id: String): Option[ContentItemStats] = stats.filter { _.id === id }.firstOption
  
  def get(ids: Traversable[String]): Traversable[ContentItemStats] = stats.filter(_.id inSet ids).list
  
  def add(s: ContentItemStats) { stats += s }
  
  def add(ss: Iterable[ContentItemStats]) { stats ++= ss }
  
  def update(s: ContentItemStats): Boolean = stats.filter { _.id === s.id }.update(s) == 1
  
  def remove(id: String): Option[ContentItemStats] = {
    val q = stats.filter{ _.id === id }
    val res = q.list.headOption
    q.delete
    res
  }
  
  /** @return a maximum 'top' items which were lastly announced longest time ago and before 'olderThan' */
  def lastAnnounced(olderThan: Long, top: Int) = {
    stats.sortBy(_.lastAnnounced).filter(_.lastAnnounced < olderThan).take(top).list
  }
     
  def getAll: List[ContentItemStats] = stats.list
  
  def count: Int = 
    //stats.length.run
    StaticQuery.queryNA[Int]("select count(*) from \"" + CIStats.TableName + "\"").first
  
  def clear() { stats.delete }
   
  def close() = session.close()
    
  // ctor
  if (MTable.getTables(CIStats.TableName).list.isEmpty) {
    stats.ddl.create    
  }
}


