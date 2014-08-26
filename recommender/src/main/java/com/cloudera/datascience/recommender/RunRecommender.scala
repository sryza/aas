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

package com.cloudera.datascience.recommender

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//import org.jblas.DoubleMatrix

object RunRecommender {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Recommender"))
    val base = "/user/spark/"
    val rawUserArtistData = sc.textFile(base + "user_artist_data.txt", 120)
    val rawArtistData = sc.textFile(base + "artist_data.txt")
    val rawArtistAlias = sc.textFile(base + "artist_alias.txt")

    preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
    model(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
    evaluate(sc, rawUserArtistData, rawArtistAlias)
  }

  def buildArtistByID(rawArtistData: RDD[String]) =
    rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }

  def buildArtistAlias(rawArtistAlias: RDD[String]): Map[Int,Int] =
    rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()

  def preparation(rawUserArtistData: RDD[String],
                  rawArtistData: RDD[String],
                  rawArtistAlias: RDD[String]) = {
    val userIDStats = rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
    val itemIDStats = rawUserArtistData.map(_.split(' ')(1).toDouble).stats()
    println(userIDStats)
    println(itemIDStats)

    val artistByID = buildArtistByID(rawArtistData)
    val artistAlias = buildArtistAlias(rawArtistAlias)

    val (badID, goodID) = artistAlias.head
    println(artistByID.lookup(badID) + " -> " + artistByID.lookup(goodID))
  }

  def buildRatings(rawUserArtistData: RDD[String],
                   artistAliasBroadcast:Broadcast[Map[Int,Int]]) = {
    rawUserArtistData.map { line =>
      val tokens = line.split(' ')
      val userID = tokens(0).toInt
      val originalArtistID = tokens(1).toInt
      val count = tokens(2).toInt
      val artistID = artistAliasBroadcast.value.getOrElse(originalArtistID, originalArtistID)
      Rating(userID, artistID, count)
    }
  }

  def model(sc: SparkContext,
            rawUserArtistData: RDD[String],
            rawArtistData: RDD[String],
            rawArtistAlias: RDD[String]): Unit = {

    val artistAliasBroadcast = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val trainData = buildRatings(rawUserArtistData, artistAliasBroadcast).cache()

    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

    trainData.unpersist()

    model.userFeatures.mapValues(java.util.Arrays.toString).take(3).foreach(println)

    val userID = 2093760
    val recommendations = model.recommendProducts(userID, 5)
    //val recommendations = recommendProducts(userID, 10, model)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val existingProductIDs = rawUserArtistData.map(_.split(' ')).
      filter(_(0).toInt == userID).map(_(1).toInt).collect().toSet

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter(idName => existingProductIDs.contains(idName._1)).
      values.collect().sorted.foreach(println)
    artistByID.filter(idName => recommendedProductIDs.contains(idName._1)).
      values.collect().sorted.foreach(println)

  }

  /*
  def recommend(recommendToFeatures: Array[Double],
                recommendableFeatures: RDD[(Int, Array[Double])],
                num: Int): Array[(Int, Double)] = {
    val recommendToVector = new DoubleMatrix(recommendToFeatures)
    val scored = recommendableFeatures.map { case (id,features) =>
      (id, recommendToVector.dot(new DoubleMatrix(features)))
    }
    scored.top(num)(Ordering.by(_._2))
  }

  def recommendProducts(user: Int, num: Int, model: MatrixFactorizationModel): Array[Rating] =
    recommend(model.userFeatures.lookup(user).head, model.productFeatures, num).
      map(t => Rating(user, t._1, t._2))
   */

  def areaUnderCurve(sc: SparkContext,
                     model: MatrixFactorizationModel,
                     positiveData: RDD[Rating]) = {
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    val positivePredictions = model.predict(positiveUserProducts).groupBy(_.user)

    val allItemIDsBC = sc.broadcast(positiveUserProducts.values.distinct().collect())

    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      userIDAndPosItemIDs => {
        val random = new Random()
        val allItemIDs = allItemIDsBC.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          // Keep about 100 negative examples per user
          while (i < allItemIDs.size && negative.size < 100) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.size))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)

    val negativePredictions = model.predict(negativeUserProducts).groupBy(_.user)

    val correctAndTotal = positivePredictions.join(negativePredictions).map {
      case (userID, (positiveRatings, negativeRatings)) =>
        var correct = 0L
        for (positive <- positiveRatings;
             negative <- negativeRatings) {
          if (positive.rating > negative.rating) {
            correct += 1
          }
        }
        (correct, positiveRatings.size * negativeRatings.size)
    }

    correctAndTotal.keys.sum / correctAndTotal.values.sum
  }

  def evaluate(sc: SparkContext,
               rawUserArtistData: RDD[String],
               rawArtistAlias: RDD[String]): Unit = {

    val artistAliasBroadcast = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val trainAndCV = rawUserArtistData.randomSplit(Array(0.8, 0.2))

    val trainData = buildRatings(trainAndCV(0), artistAliasBroadcast).cache()
    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
    trainData.unpersist()

    val cvData = buildRatings(trainAndCV(1), artistAliasBroadcast)

    val auc = areaUnderCurve(sc, model, cvData)
    printf(f"$auc")
  }

}