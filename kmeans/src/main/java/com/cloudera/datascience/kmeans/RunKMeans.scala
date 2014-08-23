/*
 * Copyright 2014 Sandy Ryza, Josh Wills, Sean Owen, Uri Laserson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.datascience.kmeans

import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object RunKMeans {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("K-means"))
    val rawData = sc.textFile("/user/spark/kddcup.data", 120)
    clusteringTake0(sc, rawData)
    clusteringTake1(sc, rawData)
  }

  // Clustering, Take 0

  def clusteringTake0(sc: SparkContext, rawData: RDD[String]): Unit = {

    rawData.map(_.split(',').last).countByValue.toSeq.sortBy(_._2).reverse.foreach(println)

    val dataAndLabel = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      (vector, label)
    }

    val data = dataAndLabel.map(_._1).cache()

    val kmeans = new KMeans()
    val model = kmeans.run(data)

    model.clusterCenters.foreach(println)

    val clusterLabelCount = dataAndLabel.map { case (datum, label) =>
      val cluster = model.predict(datum)
      (cluster, label)
    }.countByValue

    clusterLabelCount.toSeq.sorted.foreach { case ((cluster, label), count) =>
      println(f"$cluster%1s$label%18s$count%8s")
    }

    data.unpersist(true)
  }

  // Clustering, Take 1

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel) = {
    val centroid = model.clusterCenters(model.predict(datum))
    distance(centroid, datum)
  }

  def clusteringScore(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def clusteringScore2(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def clusteringTake1(sc: SparkContext, rawData: RDD[String]): Unit = {

    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }.cache()

    (5 to 30 by 5).map(k => (k, clusteringScore(data, k))).
      foreach(println)

    (30 to 100 by 10).par.map(k => (k, clusteringScore2(data, k))).
      toList.foreach(println)

    data.unpersist(true)

  }

  def visualizationInR(sc: SparkContext, rawData: RDD[String]): Unit = {

    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }.cache()

    val kmeans = new KMeans()
    kmeans.setK(100)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)

    val sample = data.map(datum =>
      model.predict(datum) + "," + datum.toArray.mkString(",")
    ).filter(_.hashCode % 20 == 0)

    sample.saveAsTextFile("/user/spark/sample")

    data.unpersist(true)

  }

  def buildNormalizedData(sc: SparkContext, data: RDD[Vector]) = {
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.take(1)(0).length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce(
      (a, b) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = dataAsArray.fold(
        new Array[Double](numCols)
      )(
        (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2)
      )
    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means = sums.map(_ / n)

    def normalize(datum: Vector) = {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0)
            (value - mean)
          else
            (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }

    data.map(normalize)
  }

  def clusteringTake2(sc: SparkContext, rawData: RDD[String]): Unit = {
    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }

    val normalizedData = buildNormalizedData(sc, data).cache()

    (60 to 120 by 10).par.map(k =>
      (k, clusteringScore2(normalizedData, k))).toList.foreach(println)

    normalizedData.unpersist(true)
  }

  def clusteringTake3(sc: SparkContext, rawData: RDD[String]): Unit = {

    val protocols = rawData.map(_.split(',')(1)).distinct().collect().zipWithIndex.toMap
    val services =  rawData.map(_.split(',')(2)).distinct().collect().zipWithIndex.toMap
    val tcpStates = rawData.map(_.split(',')(3)).distinct().collect().zipWithIndex.toMap

    val data = rawData.map { line =>
      val buffer = line.split(",").toBuffer
      val protocol = buffer.remove(1)
      val service = buffer.remove(1)
      val tcpState = buffer.remove(1)
      val label = buffer.remove(buffer.length - 1)
      val vector = buffer.map(_.toDouble)

      val newProtocolFeatures = new Array[Double](protocols.size)
      newProtocolFeatures(protocols(protocol)) = 1.0
      val newServiceFeatures = new Array[Double](services.size)
      newServiceFeatures(services(service)) = 1.0
      val newTcpStateFeatures = new Array[Double](tcpStates.size)
      newTcpStateFeatures(tcpStates(tcpState)) = 1.0

      vector.insertAll(1, newTcpStateFeatures)
      vector.insertAll(1, newServiceFeatures)
      vector.insertAll(1, newProtocolFeatures)

      Vectors.dense(vector.toArray)
    }

    val normalizedData = buildNormalizedData(sc, data).cache()

    (80 to 120 by 10).par.map(k =>
      (k, clusteringScore2(normalizedData, k))).toList.foreach(println)

    normalizedData.unpersist(true)
  }

}