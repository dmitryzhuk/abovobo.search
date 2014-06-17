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
import java.nio.Buffer
import akka.util.ByteString

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
  
  /** Safely flushes and closes the index freeing all used resources */
  def close()
}

object ContentIndex {
  type CID = String
  
  val MaxTitleLenght = 256
  val MaxDescriptionLength = 1024 * 4
  
  val Encoding = Charset.forName("UTF-8")
  
 
  sealed trait ContentRef extends Serializable {
    def id: CID
    def title: String
    def size: Long
    
    override def equals(o: Any) = o match {
      case that: ContentRef => this.id.equals(that.id)
      case _ => false
    }
    
    override def hashCode = id.hashCode

    override def toString = "ContentRef#" + id + " : " + title + " : " + size + " bytes"        
  }
  
  trait ContentItem extends ContentRef {
    def description: String
    def descriptionData: Array[Byte]
    
    def compress: CompressedContentItem
    def decompress: PlainContentItem
  }
  
  class CompressedContentItem(val id: CID, val title: String, val size: Long, val descriptionData: Array[Byte]) extends ContentItem {    
    def description: String = throw new UnsupportedOperationException("use decompress.description")

    def compress: CompressedContentItem = this
    def decompress: PlainContentItem = {
      val buf = new Array[Byte](256)
      val buffer = new ByteArrayOutputStream(512) 
      val is = plainDataInputStream
      
      Iterator.continually(is.read(buf)).takeWhile(_ != -1).foreach { read => buffer.write(buf, 0, read) }
      
      new PlainContentItem(id, title, size, buffer.toByteArray) /// XXX: remove copying
    } 
    
    private def plainDataInputStream = new InflaterInputStream(new ByteArrayInputStream(descriptionData), new Inflater(true), 256)
  }
  
  class PlainContentItem(val id: CID, val title: String, val size: Long, val descriptionData: Array[Byte]) extends ContentItem {
    if (title.length > MaxTitleLenght) throw new IllegalArgumentException("title is too long")
    if (descriptionData.length > MaxDescriptionLength) throw new IllegalArgumentException("description is too long")

    def this(id: CID, title: String, size: Long, description: String) = { 
      this(id, title, size, description.getBytes(Encoding))      
    }
    
    def description = new String(descriptionData, Encoding)
    
    def descriptionReader: Reader = new InputStreamReader(new ByteArrayInputStream(descriptionData), Encoding)
    
    def compress: CompressedContentItem = {
      new CompressedContentItem(id, title, size, ContentItem.compress(new ByteArrayInputStream(descriptionData)))
    }
    def decompress: PlainContentItem = this
  }
  
  object ContentItem {
    def apply(id: CID, title: String, size: Long, description: String): PlainContentItem = new PlainContentItem(id, title, size, description)
    
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
  }
  
  class SimpleContentRef(val id: CID, val title: String, val size: Long) extends ContentRef  
}
