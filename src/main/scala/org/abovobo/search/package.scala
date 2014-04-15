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

  /**
   * Calculates number of combinations of K elements from N.
   *
   * @param N Total number of elements.
   * @param K Number of elements in selection.
   * @return  Number of combinations.
   */
  def combinations(N: Long, K: Long): Double =
    if (K > N) 0
    else if (K == N) 1
    else
      (0L until K).foldLeft(1.0) {
        (r, k) => r * (N - k).toDouble / (k + 1).toDouble
      }

  /**
   * Calculates the probability of getting `d` common elements in 2 subsets of sizes
   * `D1` and `D2` of set `N`.
   *
   * @param N   Number of elements in the set.
   * @param D1  Number of elements in the first subset.
   * @param D2  Number of elements in the second subset.
   * @param d   Number of common elements in D1 and D2.
   * @return    Probability of having given d.
   */
  def pd(N: Long, D1: Long, D2: Long, d: Long): Double = {
    require(D1 <= N)
    require(D2 <= N)
    require(d <= D2)
    val p0 = (0L until d).foldLeft(1.0) {
      (r, i) => r * (D1 - i).toDouble / (N - i).toDouble
    }
    val p1 = (0L until (D2 - d)).foldLeft(1.0) {
      (r, i) => r * (N - D1 - i).toDouble / (N - d - i).toDouble
    }
    val c = combinations(D2, d)
    c * p0 * p1
  }

  /**
   * Calculates the distribution of probabilities defined by method [[org.abovobo.search.pd()]] above.
   *
   * @param N   Number of elements in the set.
   * @param D1  Number of elements in the first subset.
   * @param D2  Number of elements in the second subset.
   * @return    Sequence of probabilities where each element represents a probability of having `index`
   *            common elements in subsets D1 and D2.
   */
  def p(N: Long, D1: Long, D2: Long): IndexedSeq[Double] = (0L to D2).map(pd(N, D1, D2, _))
}
