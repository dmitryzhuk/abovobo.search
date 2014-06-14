package org.abovobo.search.impl

import org.abovobo.search.ContentIndex
import org.abovobo.search.ContentIndex._
import org.apache.lucene.util.Version
import java.io.File
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.NoLockFactory
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.document.Field
import org.apache.lucene.document.Document
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.LongField
import org.apache.lucene.search.TermQuery
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.QueryParser
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.Path
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Sort
import org.apache.lucene.index.IndexReader

/**
 * Lucene based impl of content index. Note: this impl is not thread safe.
 */
class LuceneContentIndex(val indexLocation: Path) extends ContentIndex {
  import LuceneContentIndex._
  
  val directory = FSDirectory.open(indexLocation.toFile, NoLockFactory.getNoLockFactory)
  
  private val analyzer = {
    //new EnglishAnalyzer(version)
    new StandardAnalyzer(version, StandardAnalyzer.STOP_WORDS_SET)
  }
  
  private val writer = {
    val config = new IndexWriterConfig(version, analyzer)
    new IndexWriter(directory, config)
  }
  private var reader: DirectoryReader = DirectoryReader.open(directory)
  private var indexSearcher: IndexSearcher = new IndexSearcher(reader)
  private var changesCount = 0
    
  def add(item: ContentItem) {
    val doc = createDocument(item)
    writerAction { writer => writer.addDocument(doc, analyzer) }
  }
  
  def remove(id: CID) {
    writerAction { writer => writer.deleteDocuments(new TermQuery(new Term("infohash", id))) }
  }
  
  def list: Traversable[ContentRef] = {
    lookup("*")
  }

  def contains(id: CID): Boolean = {
      throw new RuntimeException("not implemented")
  }

  def lookup(searchExpression: String): Traversable[ContentRef] = {
    val qp = new QueryParser(version, "default", analyzer)
    qp.setAllowLeadingWildcard(true)
    val query = qp.parse(searchExpression)
    val topDocs = indexSearcher.search(query, maxResults)
    
    topDocs.scoreDocs map { sd =>
      val doc = indexSearcher.doc(sd.doc)
      SimpleContentRef(doc.get("infohash"), doc.get("title"))
    }
  }
      
  def clear() {
    writer.deleteAll()
    writer.commit()
  }

  def count: Int = reader.getDocCount("infohash")
  
  def size: Long = Files.size(indexLocation)
  
  def close() {
    reader.close()
    writer.close()
    directory.close()
  }
  
  private def resetReader() {
    val newReader = DirectoryReader.openIfChanged(reader)
    if (newReader != null) {
      val t = reader
      reader = newReader
      indexSearcher = new IndexSearcher(reader)
      t.close() // we can't handle io exception correctly here..
    }
  }
  
  private def createDocument(item: ContentItem): Document = {
    val infohash = new StringField("infohash", item.id, Store.YES)    
    val title = new TextField("title", item.title, Store.YES)
    val description = new TextField("description", item.description, Store.NO)
    val size = new LongField("size", item.size, Store.YES)
    
    val defaultValue = new StringBuilder(item.description.length + item.title.length + 64)
    
    defaultValue.append(item.id).append(" ").append(item.title).append(" ").append(item.description).append(" size ").append(item.size).append(" bytes")
    
    val default = new TextField("default", defaultValue.toString, Store.NO) // having things we still need separate field 'description' to allow specific search on it
    
    val doc = new Document()
    doc.add(infohash)
    doc.add(title)
    doc.add(description)
    doc.add(size)
    doc.add(default)
    
    doc
  }
  
  private def writerAction(action: IndexWriter => Any) {
    try {
      action(writer)
      changesCount += 1
      if (changesCount > commitThreshold) {
        writer.commit()
        changesCount = 0
        //resetReader()
      }      
    } catch {
      case t: Throwable =>
        writer.rollback()
        throw t
    }
  }
}

object LuceneContentIndex {
  val version = Version.LUCENE_48
  val commitThreshold = 0
  val maxResults = 50
}