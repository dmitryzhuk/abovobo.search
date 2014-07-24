package org.abovobo.search.impl

import java.sql.Connection
import org.abovobo.search.IndexManagerRegistry
import org.abovobo.search.IndexManagerRegistry.ContentItemStats
import java.sql.ResultSet
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager

object H2IndexManagerRegistry {
  Class.forName("org.h2.Driver")

  def apply(uri: String): H2IndexManagerRegistry = {
    import org.abovobo.jdbc.Closer._

    val connection = DriverManager.getConnection(uri, "", "")
    
    using(connection.createStatement()) { st =>
      st.executeUpdate("create table if not exists stats (id varchar(40) primary key, hits int, announces int, added long, lastAnnounced long, lastHit long)")
    }
    
    new H2IndexManagerRegistry(connection)
  }
}

class H2IndexManagerRegistry(connection: Connection) extends IndexManagerRegistry {
  import org.abovobo.jdbc.Closer._
  
  def get(id: String): Option[ContentItemStats] = {
    getStatement.setString(1, id)
    using(getStatement.executeQuery()) { rs =>
      if (rs.next()) Some(this.read(rs)) else None
    }    
  }

  def get(ids: Traversable[String]): Traversable[ContentItemStats] = ids.flatMap { id => get(id) }

  def add(s: ContentItemStats) = {
    mergeStatement.setString(1, s.id)
    mergeStatement.setInt(2, s.hits)
    mergeStatement.setInt(3, s.announces)
    mergeStatement.setLong(4, s.added)
    mergeStatement.setLong(5, s.lastAnnounced)
    mergeStatement.setLong(6, s.lastHit)
    mergeStatement.executeUpdate()
  }
  
  def add(ss: Iterable[ContentItemStats]) = ss.foreach(add)
  
  def update(s: ContentItemStats): Boolean = { add(s); true }
  
  def remove(id: String): Option[ContentItemStats] = {
    val t = get(id)
    removeStatent.setString(1, id)
    removeStatent.executeUpdate()
    t
  }
  
  def lastAnnounced(olderThan: Long, top: Int): Iterable[ContentItemStats] = {
    lastAnnouncedStatement.setLong(1, olderThan)
    lastAnnouncedStatement.setInt(2, top)
    val result = ArrayBuffer.empty[ContentItemStats]
    using(lastAnnouncedStatement.executeQuery()) { rs =>
      while (rs.next()) {
        result.append(read(rs))
      }
    }  
    result
  }
  
  def stats: Iterable[ContentItemStats] = {
    using(connection.createStatement()) { s =>
      using(s.executeQuery("select * from stats")) { rs =>
        Stream.continually(rs).takeWhile(_.next).map(read).toSeq
      }
    }
  }
  
  def count: Int = using(countStatement.executeQuery()) { rs => rs.next(); rs.getInt(1) }
  
  def clear() {
    clearStatement.executeUpdate()
  }
  
  def close() = connection.close()
  
  private def read(rs: ResultSet): ContentItemStats = {
    new ContentItemStats(
        rs.getString("id"),
        rs.getInt("hits"),
        rs.getInt("announces"),
        rs.getLong("added"),
        rs.getLong("lastAnnounced"),
        rs.getLong("lastHit"))
  }
  
  val getStatement = connection.prepareStatement("select * from stats where id=?")
  val mergeStatement = connection.prepareStatement("merge into stats (id, hits, announces, added, lastAnnounced, lastHit) key(id) values(?,?,?,?,?,?)")
  val removeStatent = connection.prepareStatement("delete from stats where id=?")
  val lastAnnouncedStatement = connection.prepareStatement("select * from stats where lastAnnounced < ? order by lastAnnounced limit ?")
  val countStatement = connection.prepareStatement("select count(*) from stats")
  val clearStatement = connection.prepareStatement("delete from stats")
}
