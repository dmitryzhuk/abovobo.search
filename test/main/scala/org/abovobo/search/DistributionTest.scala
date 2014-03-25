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

/** Tests distribution parameters calculator (M,K) */
object DistributionTest extends App {

  val N = Array(100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000)

  for (n <- N) {
    println(n + ";" + distribution(n))
  }

}
