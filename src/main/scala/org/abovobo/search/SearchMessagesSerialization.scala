package org.abovobo.search

import SearchPlugin._
import akka.util.ByteString
import org.abovobo.conversions.Bencode
import org.abovobo.search.ContentIndex._
import java.nio.charset.Charset
import akka.util.ByteStringBuilder

object SearchMessagesSerialization {
  def deserializeMessage(msg: ByteString): SearchMessage = {
    val events = Bencode.decode(msg)

    def xthrow(code: Int, message: String) = throw new RuntimeException("error " + code + ": " +  message)

    def array(event: Bencode.Event): Array[Byte] = event match {
      case Bencode.Bytestring(value) => value
      case _ => xthrow(0, "Malformed packet")
    }

    def string(event: Bencode.Event): String = event match {
      case Bencode.Bytestring(value) => new String(value, "UTF-8")
      case _ => xthrow(0, "Malformed packet")
    }

    def integer(event: Bencode.Event): Long = event match {
      case Bencode.Integer(value) => value
      case _ => xthrow(0, "Malformed packet")
    }
    
    def next(): Bencode.Event = events.next()
    
    def ensure(ec: Class[_ <: Bencode.Event]) = {
      val n = next()
      if (!ec.isInstance(n)) {
        xthrow(2, "Illegal arguments")         
      }
    }
    
    ensure(classOf[Bencode.DictionaryBegin])
        
    string(next()) match {
      case "a" => // Announce
        next() // list
        val id = string(next())
        val title = string(next())
        val size = integer(next())
        val description = array(next())
        val ttl = integer(next())
        val width = integer(next())
        ensure(classOf[Bencode.ListEnd])
        Announce(new CompressedContentItem(id, title, size, description), new AnnounceParams(ttl.toInt, width.toInt))
      case "l" => // Lookup
        next() // list
        val searchString = string(next())
        val ttl = integer(next())
        val width = integer(next())
        ensure(classOf[Bencode.ListEnd])
        Lookup(searchString, new SearchParams(ttl.toInt, width.toInt))
      case "f" => // FoundItems
        next() // list
        val searchString = string(next())
        // lists with items
        val items = scala.collection.mutable.Set.empty[ContentRef]
        while (next().isInstanceOf[Bencode.ListBegin]) {
          items += new SimpleContentRef(string(next()), string(next()), integer(next()))
          ensure(classOf[Bencode.ListEnd])
        }
        // ensure(classOf[Bencode.ListEnd]) - discarded in while
        FoundItems(searchString, items)
      case "s" => // SearchFinished
        SearchFinished(string(next()))
      case "e" => // Error
        next() // list
        val e = Error(integer(next()).toInt, string(next()))
        ensure(classOf[Bencode.ListEnd])
        e
      case "x" => // FloodClearIndx
        FloodClearIndex
      case x => xthrow(1, "Unknown message type " + x)
    }
  }
  
  def serializeMessage(msg: SearchMessage): ByteString = {
    val buf = new ByteStringBuilder()
    buf += 'd'
    
    val charset = Charset.forName("UTF-8")
    def putStr(str: String) {
      putArray(str.getBytes(charset))
    }
    def putNum(i: Long) {
      buf += 'i' ++= i.toString.getBytes(charset) += 'e'
    } 
    def putArray(bytes: Array[Byte]) {
      buf ++= bytes.length.toString.getBytes(charset) += ':' ++= bytes 
    }
    def putChar(ch: Byte) = {
      buf += '1' += ':' += ch
    }
      
    msg match {
      case Announce(item, params) => 
        putChar('a')
        buf += 'l'
          putStr(item.id)
          putStr(item.title)
          putNum(item.size)
          putArray(item.compress.descriptionData)
          putNum(params.ttl)
          putNum(params.width)
        buf += 'e'
      case Lookup(searchString, params) =>          
        putChar('l')
        buf += 'l'
          putStr(searchString)
          putNum(params.ttl)
          putNum(params.width)
        buf += 'e'          
      case FoundItems(str, found) =>
        putChar('f')
        buf += 'l'
          putStr(str)            
          found.foreach { item => 
            buf += 'l'
              putStr(item.id)
              putStr(item.title)
              putNum(item.size)
            buf += 'e'               
          }  
        buf += 'e' 
      case SearchFinished(str) =>
        putStr("s")
        putStr(str)
      case Error(code, text) =>
        putChar('e')
        buf += 'l'
          putNum(code)
          putStr(text)
        buf += 'e'
      case FloodClearIndex =>
        putChar('x')
        putChar('x')
      case ClearIndex =>
        throw new IllegalStateException("Local-only command")
    }
    buf += 'e'  
    buf.result()
  }
}