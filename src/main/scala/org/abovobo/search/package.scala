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

}
