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


  /*
  // fixed size of D1 subset
  val D2 = 50
  // number of peaks on D2 distribution
  val points = 10
  // width of the curve
  val width = D2 / points
  // probability value interpreted as the end of the `bell`
  val threshold = 10E-3

  for (i <- 0 until points) {
    val peak = width * (points - i) - width / 2
    val right = width * (points - i)
    val left = width * (points - i - 1)

    def shape(data: IndexedSeq[Double]): Tuple3[Long, Long, Long] = {
      var peak0 = -1
      var peakval = 0.0
      var left0 = -1
      var leftval = 0.0
      var right0 = -1
      var rightval = 0.0
      for (k <- 0 to D2) {
        if (data(k) > peakval) {
          peakval = data(k)
          peak0 = k
          if (data(k) < threshold) {
            leftval = data(k)
            left0 = k
          }
        } else {
          if (data(k) > threshold) {
            rightval = data(k)
            right0 = k
          }
        }
      }
      (peak0, left0, right0)
    }


    var matched = false
    var N = D2 * 2
    var D1 = (D2 * 1.5).toLong

    while (!matched) {

      val p0 = p(N, D1, D2)
      val shape0 = shape(p0)

      println(shape0 + ";" + D1 + ";" + N)
      if (shape0._1 > peak) {
        N = N + Math.max(1, (N * 0.01).toInt)
      } else if (shape0._1 < peak) {
        N = N - Math.max(1, (N * 0.01).toInt)
      } else {
        // Here shaped peak is equal to measured
        if (shape0._2 < left) {
          D1 = D1 - Math.max(1, (D1 * 0.01).toInt)
        }
      }
      //println(p0.map(_.toString).reduceLeft(_ + "\n" + _))
    }
  }

*/


  /*
  val N = Array(1000, 10000, 100000, 1000000, 10000000, 15000000, 100000000, 1000000000)
  val D = 300

  for (n <- N) {
    val dd = p(n, D, D)
    println(n + ";" + dd.reduceLeft(_ + _) + ";" + dd)
  }
  */

  //println(pd(1000, 300, 300, 10))


  val p1000 = p(1200000, 40000, 1000)
  //println(p1000.drop(70).take(40).reduceLeft(_ + _))
  println(p1000.map(_.toString).reduceLeft(_ + "\n" + _))

  /*
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
  */

}
