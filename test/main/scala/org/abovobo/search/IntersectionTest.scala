/**
 * Abovobo DSA Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.search

import scala.util.Random
import scala.collection.mutable

/** Tests propagation parameters calculator (alpha,tau) */
object IntersectionTest extends App {

  /*
  val N = Array(1000, 10000, 100000, 1000000, 10000000, 15000000, 100000000, 1000000000)
  val D = 300

  for (n <- N) {
    val dd = p(n, D, D)
    println(n + ";" + dd.reduceLeft(_ + _) + ";" + dd)
  }
  */

  //println(pd(1000, 300, 300, 10))


  //val p1000 = p(440, 300, 300)
  //println(p1000.drop(70).take(40).reduceLeft(_ + _))
  //println(p1000.reduceLeft(_ + _) + ";\n" + p1000.map(_.toString).reduceLeft(_ + "\n" + _))

  val N = 1000
  //val nodes = new Array[Integer160](N)
  //for (i <- 0 until N) nodes(i) = Integer160.random
  val D = 300
  val Z = 30000
  val intersections = new Array[Long](Z)
  val probabilities = new Array[Double](D + 1)
  val rand = new Random(System.currentTimeMillis())

  for (i <- 0 until Z) {
    val d1 = new  mutable.HashSet[Int]()
    val d2 = new  mutable.HashSet[Int]()
    while (d1.size < D) d1 += rand.nextInt(N)
    while (d2.size < D) d2 += rand.nextInt(N)

    intersections(i) = (d1 & d2).size
  }

  for (i <- 0 to D) {
    probabilities(i) = intersections.filter(_ == i).size.toDouble / Z.toDouble
  }
  println(probabilities.map(_.toString).reduceLeft(_ + "\n" + _))

}
