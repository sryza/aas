/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.datascience.risk

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter
import org.apache.commons.math3.util.FastMath

object KernelDensity {
  def chooseBandwidth(samples: Seq[Double]): Double = {
    val stddev = new StatCounter(samples).stdev
    1.06 * stddev * math.pow(samples.size, -.2)
  }

  def chooseBandwidth(samples: RDD[Double]): Double = {
    val stats = samples.stats()
    1.06 * stats.stdev * math.pow(stats.count, -.2)
  }

  def estimate(samples: Seq[Double], evaluationPoints: Array[Double], bandwidth: Double)
    : Array[Double] = {
    val stddev = if (bandwidth < 0) chooseBandwidth(samples) else bandwidth
    val logStandardDeviationPlusHalfLog2Pi =
      FastMath.log(stddev) + 0.5 * FastMath.log(2 * FastMath.PI)

    val zero = (new Array[Double](evaluationPoints.length), 0)
    val (points, count) = samples.aggregate(zero)(
      (x, y) => mergeSingle(x, y, evaluationPoints, stddev, logStandardDeviationPlusHalfLog2Pi),
      (x1, x2) => combine(x1, x2, evaluationPoints))

    var i = 0
    while (i < points.length) {
      points(i) /= count
      i += 1
    }
    points
  }

  /**
   * Given a set of samples form a distribution, estimates its density at the set of given points.
   * Uses a Gaussian kernel with the given standard deviation.
   */
  def estimate(samples: RDD[Double], evaluationPoints: Array[Double], bandwidth: Double)
    : Array[Double] = {
    val stddev = if (bandwidth < 0) chooseBandwidth(samples) else bandwidth
    val logStandardDeviationPlusHalfLog2Pi =
      FastMath.log(stddev) + 0.5 * FastMath.log(2 * FastMath.PI)

    val zero = (new Array[Double](evaluationPoints.length), 0)
    val (points, count) = samples.aggregate(zero)(
      (x, y) => mergeSingle(x, y, evaluationPoints, stddev, logStandardDeviationPlusHalfLog2Pi),
      (x1, x2) => combine(x1, x2, evaluationPoints))

    var i = 0
    while (i < points.length) {
      points(i) /= count
      i += 1
    }
    points
  }

  private def mergeSingle(
      x: (Array[Double], Int),
      y: Double,
      evaluationPoints: Array[Double],
      standardDeviation: Double,
      logStandardDeviationPlusHalfLog2Pi: Double): (Array[Double], Int) = {
    var i = 0
    while (i < evaluationPoints.length) {
      x._1(i) += normPdf(y, standardDeviation, logStandardDeviationPlusHalfLog2Pi,
        evaluationPoints(i))
      i += 1
    }
    (x._1, i)
  }

  private def combine(
      x: (Array[Double], Int),
      y: (Array[Double], Int),
      evaluationPoints: Array[Double]): (Array[Double], Int) = {
    var i = 0
    while (i < evaluationPoints.length) {
      x._1(i) += y._1(i)
      i += 1
    }
    (x._1, x._2 + y._2)
  }

  private def normPdf(mean: Double, standardDeviation: Double,
      logStandardDeviationPlusHalfLog2Pi: Double, x: Double): Double = {
    val x0 = x - mean
    val x1 = x0 / standardDeviation
    FastMath.exp(-0.5 * x1 * x1 - logStandardDeviationPlusHalfLog2Pi)
  }
}
