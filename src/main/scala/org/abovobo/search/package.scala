/**
 * Abovobo DSA Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo

import scala.annotation.tailrec

/**
 * Collection of functions useful for genral statistic calculations related to distributed search.
 */
package object search {

  /**
   * Caclculates the probability of not finding the item with given search parameters.
   *
   * @param N Overall number of nodes in the network.
   * @param K Knowledge distribution: number of nodes having a content index.
   * @param M Number of nodes queried with search query.
   * @return  A probability of not finding the content item.
   */
  def probability(N: Long, K: Long, M: Long): Double =
    if ((M + K) > N) 0
    else
      (0L until M).foldLeft(1.0) {
        (result, i) =>
          result * ((N - K - i).toDouble / (N - i).toDouble)
      }


  /**
   * Calculates magnitude of given number. Returns zero for 0. For numbers 1..99 returns 1,
   * for numbers 100..999 returns 10 etc. For negative numbers returned value will be negative.
   *
   * @param v A number to get magnitude of.
   * @return  A magnitude of number.
   */
  def magnitude(v: Long): Long = {
    @tailrec
    def _magnitude(v: Long, m: Long): Long = if (v / m < 10) m else _magnitude(v, m * 10)
    if (v == 0) 0 else _magnitude(v, if (v > 0) 1 else -1)
  }

  /**
   * Returns pair of numbers M and K which a sufficient to acvhieve target probability
   * of finding the content item.
   *
   * @param N       An estimated number of nodes in the network.
   * @param epsilon A probability of *not* finding the item (lower is better).
   * @return        Proposed values for K and M parameters.
   */
  def distribution(N: Long, epsilon: Double = 0.1): (Long, Long) = {

    var min = Long.MaxValue
    var k, m = 2L
    var result = (m, k)

    while (k + m < N) {
      val p = probability(N, k, m)
      if (p < epsilon) {
        if (m + k < min) {
          min = m + k
          result = (m, k)
        } else {
          m = N
        }
      }
      if (k < m) k = k + k / 2 else m = m + m / 2
    }

    result
  }

  /**
   * Calculates propagation parameters alpha and TTL (tau) for given value X.
   *
   * @param X Number of nodes to propagate search query or content index to.
   * @return  Pair of alpha and tau to meet given number.
   */
  def propagation(X: Long): (Long, Long) = {
    var alpha, tau = 2L
    var min = Long.MaxValue
    var result = (alpha, tau)

    while (alpha + tau <= min) {
      if (Math.pow(alpha, tau) < X) {
        tau += 1
      } else {
        result = (alpha, tau)
        min = alpha + tau
        tau = 2
        alpha += 1
      }
    }

    result
  }

  def pD(N: Long, D: Long): Double = D * (0L until D).foldLeft(1.0) { (r, i) => r * (D - i) / (N - i) }

  def p0(N: Long, D: Long): Double = (0L until D).foldLeft(1.0) { (r, i) => r * (N - D - i) / (N - i) }

  /*
  def pdd(N: Long, D: Long, d: Long): Double =
    d *
      (0L until d).foldLeft(1.0) { (r, i) => r * (D - i) / (N - i) } *
      (0L until (D - d)).foldLeft(1.0) { (r, i) => r * (N - D - i) / (N - d - i) }
      */

  def pdd(N: Long, D: Long, d: Long): Double =
    (1L to d).foldLeft(1.0) { (r, i) => r * (D - d + i) / (N - d + i) }

  def f0(N: Long, D: Long): Seq[Double] = p0(N, D) :: (1L until D).map(pdd(N, D, _)).toList ::: List(pD(N, D))
}
