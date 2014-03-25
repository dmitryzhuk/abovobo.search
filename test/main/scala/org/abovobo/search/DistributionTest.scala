package org.abovobo.search

/**
 * Created by dmitryzhuk on 21.03.14.
 */
object DistributionTest extends App {

  val N = Array(100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000)

  for (n <- N) {
    println(n + ";" + distribution(n))
  }

}
