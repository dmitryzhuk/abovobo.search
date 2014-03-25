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
 * Created by dmitryzhuk on 21.03.14.
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

  def magnitude(v: Long): Long = {
    @tailrec
    def _magnitude(v: Long, m: Long): Long = if (v / m < 10) m else _magnitude(v, m * 10)
    _magnitude(v, 1)
  }

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
}
