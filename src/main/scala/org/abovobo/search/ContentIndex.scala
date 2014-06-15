package org.abovobo.search

import akka.actor.Actor
import org.abovobo.integer.Integer160
import ContentIndex._
import java.nio.charset.Charset
import java.io.InputStreamReader
import java.io.ByteArrayInputStream
import java.io.Reader
import java.util.zip.DeflaterInputStream
import java.util.zip.Deflater
import java.nio.file.Files
import java.nio.CharBuffer
import java.io.CharArrayWriter
import scala.io.Source
import scala.io.Codec
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.util.zip.DeflaterOutputStream
import java.util.zip.InflaterInputStream
import java.util.zip.Inflater

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
  val MaxDescriptionLength = 1024 * 4
  
  val Encoding = Charset.forName("UTF-8")
  
 
  sealed trait ContentRef extends Serializable {
    def id: CID
    
    override def equals(o: Any) = o match {
      case that: ContentRef => this.id.equals(that.id)
      case _ => false
    }
    
    override def hashCode = id.hashCode
    
    override def toString = "ContentRef(#" + id + ")" 
  }
  
  class ContentItem private(val id: CID, val title: String, val descriptionData: Array[Byte], val size: Long) extends ContentRef {
    if (title.length > MaxTitleLenght) throw new IllegalArgumentException("title is too long")
    if (descriptionData.length > MaxDescriptionLength) throw new IllegalArgumentException("description is too long")
    
    def this(id: CID, title: String, description: String, size: Long) = { 
      this(id, title, ContentItem.compress(description), size)      
    }
    
    def description = {
      val reader = descriptionReader
      val buffer = new CharArrayWriter(256)
      val buf = new Array[Char](256)      
      Iterator.continually(reader.read(buf)).takeWhile { _ != -1 } foreach { read => buffer.write(buf, 0, read) }
      buffer.toString
    }
    
    def descriptionReader: Reader = new InputStreamReader(plainDataInputStream, Encoding)

    private def plainDataInputStream = new InflaterInputStream(new ByteArrayInputStream(descriptionData), new Inflater(true), 256)
    
    override def toString = id + " : " + title + " : " + size + " bytes"
  }
  
  object ContentItem {
    def compress(str: String): Array[Byte] = {
      val bytes = str.getBytes(Encoding)
      compress(new ByteArrayInputStream(bytes), bytes.length)
    }
    
    def compress(is: InputStream, estimatedSize: Int = 256): Array[Byte] = {
      val os = new ByteArrayOutputStream(estimatedSize)
      val zos = new DeflaterOutputStream(os, new Deflater(9, true), 256)
      val buf = new Array[Byte](256)
      Iterator.continually(is.read(buf)).takeWhile(_ != -1).foreach { read => zos.write(buf, 0, read) }
      zos.close
      os.toByteArray
    }
    
    def wrap(id: CID, title: String, compressedDescription: Array[Byte], size: Long): ContentItem = {
      new ContentItem(id, title, compressedDescription, size)
    }
  }
  
  class SimpleContentRef(val id: CID, val title: String) extends ContentRef {
    override def toString = "SimpleContentRef(#" + id + ", title: " + title  + ")"     
  }
  class RatedContentRef(val id: CID, val title: String, val timestamp: Long, val hits: Long) extends ContentRef  
}
