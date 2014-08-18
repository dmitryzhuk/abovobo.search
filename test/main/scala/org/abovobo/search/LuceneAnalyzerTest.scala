package org.abovobo.search

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute}
import org.apache.lucene.util.Version
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

/**
 * Some experimenting with Lucene Analyzers
 */
class LuceneAnalyzerTest extends WordSpec with Matchers with BeforeAndAfterAll {


  "An analyzer" when {
    "allala" must {
      "ololo" in {
        val analyzer = new RussianAnalyzer(Version.LUCENE_4_9)
        val stream = analyzer.tokenStream("name", "Это русский текст посвящённый посвященный this is an English insertion: Ивану Ивановичу Сидорову, в котором мы попытаемся осознать происходящее")
        stream.reset()
        while (stream.incrementToken()) {
          val token = stream.getAttribute(classOf[CharTermAttribute])
          println(token.toString)
        }
        stream.close()
      }
    }
  }
}
