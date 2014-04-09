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

/** Tests propagation parameters calculator (alpha,tau) */
object IntersectionTest extends App {

  val N = Array(100, 1000, 10000, 100000, 1000000, 10000000, 15000000, 100000000, 1000000000)
  val D = 3000

  for (n <- N) {
    /*val distr = distribution(n)
    val propM = propagation(distr._1)
    val propK = propagation(distr._2)*/
    val dd = f0(n, D)
    println(n + ";" + dd.reduceLeft(_ + _) + ";" + dd)
  }

}
