package org.abovobo.search

import org.abovobo.logging.global.AbovoboLogMessage
import org.abovobo.logging.global.AbovoboLogMessage._
import org.abovobo.logging.global.AbovoboLogMessage.Scope._
import org.abovobo.integer.Integer160
import org.abovobo.search.SearchPlugin.SearchMessage

object SearchLogMessage {  
  def in(self: Integer160, cmd: SearchMessage, from: Integer160) = 
    new SearchLogMessage(self, cmd.toString, In, Map("remote" -> from))

  def out(self: Integer160, cmd: SearchMessage, to: Integer160) = 
    new SearchLogMessage(self, cmd.toString, Out, Map("remote" -> to))
  
  def multi(self: Integer160, cmd: SearchMessage, to: Iterable[Integer160]) =
    new SearchLogMessage(self, cmd.toString, Out, Map("remote" -> to.mkString(",")))

  def local(self: Integer160, topic: String, args: (String, Object)*) = 
    new SearchLogMessage(self, topic, Local, args.toMap)
}

class SearchLogMessage(self: Integer160, topic: String, scope: Scope, arguments: scala.collection.Map[String, Object] = Map())
  extends AbovoboLogMessage(self, topic, scope, arguments) {
}

