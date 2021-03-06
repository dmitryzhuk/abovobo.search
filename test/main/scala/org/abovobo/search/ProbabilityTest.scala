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

/** Tests content item finding probability calculator */
object ProbabilityTest extends App {

  val N = Array(100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000)
  val K = Range(0, 10000, 100)
  val M = Range(0, 10000, 100)

  for (n <- N) {
    for (k <- K) {
      for (m <- M) {
        val ka = if (k == 0) 1 else k
        val em = if (m == 0) 1 else m
        println(n + ";" + ka + ";" + em + ";" + (1 - probability(n, ka, em)))
      }
    }
  }

}
