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
    stats.sortBy(_.lastAnnounced).where(_.lastAnnounced < olderThan).take(top).list
  }
     
  def getAll: List[ContentItemStats] = stats.list
  
  def count: Int = StaticQuery.queryNA[Int]("select count(*) from \"" + IndexManagerRegistry.TableName + "\"").first
  
  def clear() { stats.delete }
   
  def close() = session.close()
    
  // ctor
  if (MTable.getTables(IndexManagerRegistry.TableName).list.isEmpty) {
    stats.ddl.create    
  }
}


object IndexManagerRegistry extends App {
  val TableName = "ContentItemStats"
  
  case class ContentItemStats(id: String, hits: Int = 0, announces: Int = 0, added: Long = System.currentTimeMillis, lastAnnounced: Long = System.currentTimeMillis, lastHit: Long = System.currentTimeMillis)

  class CIStats(tag: Tag) extends Table[ContentItemStats](tag, TableName) {      
    def id = column[String]("ID", O.PrimaryKey)

    def hits = column[Int]("hits", O.Default(0))
    def announces = column[Int]("announces", O.Default(0))
    
    def added = column[Long]("added")
    def lastAnnounced = column[Long]("lastAnnounced") 
    
    def lastHit = column[Long]("lastHit")
    
    def * = (id, hits, announces, added, lastAnnounced, lastHit) <> (ContentItemStats.tupled, ContentItemStats.unapply)
  }

  
//  implicit def integer160ToString(i: Integer160) = i.toString
//  def now = System.currentTimeMillis
//
//        
//  if (false) {
//    // test
//    
//    val db = Database.forURL("jdbc:h2:mem:stats", driver = "org.h2.Driver")
//    
//    
//    val stats: TableQuery[CIStats] = TableQuery[CIStats]
//
//    db.withSession { implicit session => 
//      stats.ddl.create
//      
//      val ref = Integer160.random.toString
//      
//      stats += ContentItemStats(ref, 10, 20, now, now)
//      stats += ContentItemStats(Integer160.random, 11, 21, now, now)
//      stats += ContentItemStats(Integer160.random, 12, 22, now, now)
//      
//    
//      println(stats.list)
//
//      stats.filter(_.id === ref).delete
//      
//      
//      stats += ContentItemStats(Integer160.random, 15, 25, now, now)
//      
//      println(stats.list)
//    }
//  }
//  
//  {
//    // test 2
//    val registry = new IndexManagerRegistry("jdbc:h2:~/db/stats")
//    
//    val id = Integer160.random
//    
//    registry.clear()
//    
//    registry.add(ContentItemStats(id, 10, 20, now, now))
//    registry.add(ContentItemStats(id + 1, 11, 20, now, now))
//    registry.add(ContentItemStats(id + 2, 12, 20, now, now))
//    
//    println("count")
//    println(registry.count)
//
//    println("get: " + registry.get(id))
//    
//    val oldVal = registry.get(id).get
//    
//    val newVal = oldVal.copy(hits = oldVal.hits + 7)
//    
//    registry.update(newVal)
//    
//    println("updated")
//    println(registry.getAll)
//
//
//    println("deleted")
//
//    registry.remove(id)
//    
//    println("get: " + registry.get(id))
//
//    println(registry.getAll)
//
//    registry.close()
//  }
}