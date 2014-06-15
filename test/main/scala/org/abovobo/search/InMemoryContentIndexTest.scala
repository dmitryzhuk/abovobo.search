package org.abovobo.search

import org.abovobo.search.impl.InMemoryContentIndex

class InMemoryContentIndexTest extends ContentIndexTest {
  def newIndex = new InMemoryContentIndex
}