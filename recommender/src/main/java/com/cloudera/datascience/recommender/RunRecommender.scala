/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.recommender

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object RunRecommender {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Recommender"))
    val base = "hdfs:///user/ds/"
    val rawUserArtistData = sc.textFile(base + "user_artist_data.txt")
    val rawArtistData = sc.textFile(base + "artist_data.txt")
    val rawArtistAlias = sc.textFile(base + "artist_alias.txt")

    preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
    model(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
    evaluate(sc, rawUserArtistData, rawArtistAlias)
    recommend(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
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
                   artistAliasBC: Broadcast[Map[Int,Int]]) = {
    rawUserArtistData.map { line =>
      val tokens = line.split(' ')
      val userID = tokens(0).toInt
      val originalArtistID = tokens(1).toInt
      val count = tokens(2).toInt
      val artistID = artistAliasBC.value.getOrElse(originalArtistID, originalArtistID)
      Rating(userID, artistID, count)
    }
  }

  def model(sc: SparkContext,
            rawUserArtistData: RDD[String],
            rawArtistData: RDD[String],
            rawArtistAlias: RDD[String]): Unit = {

    val artistAliasBC = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val trainData = buildRatings(rawUserArtistData, artistAliasBC).cache()

    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

    trainData.unpersist()

    model.userFeatures.mapValues(java.util.Arrays.toString).take(3).foreach(println)

    val userID = 2093760
    val recommendations = model.recommendProducts(userID, 5)
    recommendations.foreach(println)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val existingProductIDs = rawUserArtistData.map(_.split(' ')).
      filter(_(0).toInt == userID).map(_(1).toInt).collect().toSet

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter(idName => existingProductIDs.contains(idName._1)).
      values.collect().sorted.foreach(println)
    artistByID.filter(idName => recommendedProductIDs.contains(idName._1)).
      values.collect().sorted.foreach(println)

  }

  def areaUnderCurve(positiveData: RDD[Rating],
                     allItemIDsBC: Broadcast[Array[Int]],
                     predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
    // Take held-out data as the "positive", and map to tuples
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    // Make predictions for each of them, including a numeric score, and gather by user
    val positivePredictions = predictFunction(positiveUserProducts).map((_.rating, 1.0))

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other items, excluding those that are "positive" for the user.
    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      // mapPartitions operates on many (user,positive-items) pairs at once
      userIDAndPosItemIDs => {
        // Init an RNG and the item IDs set once for partition
        val random = new Random()
        val allItemIDs = allItemIDsBC.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          // Keep about as many negative examples per user as positive.
          // Duplicates are OK
          while (i < allItemIDs.size && negative.size < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.size))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          // Result is a collection of (user,negative-item) tuples
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)
    // flatMap breaks the collections above down into one big set of tuples

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeUserProducts).map((_.rating, 0.0))

    val metrics = new BinaryClassificationMetrics(positivePredictions.union(negativePredictions))
    metrics.areaUnderROC()
  }

  def predictMostListened(sc: SparkContext, train: RDD[Rating])(allData: RDD[(Int,Int)]) = {
    val listenCountBC =
      sc.broadcast(train.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap())
    allData.map { case (user, product) =>
      Rating(user, product, listenCountBC.value.getOrElse(product, 0.0))
    }
  }

  def evaluate(sc: SparkContext,
               rawUserArtistData: RDD[String],
               rawArtistAlias: RDD[String]): Unit = {
    val artistAliasBC = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val allData = buildRatings(rawUserArtistData, artistAliasBC)
    val trainAndCV = allData.randomSplit(Array(0.9, 0.1))

    val trainData = trainAndCV(0).cache()
    val cvData = trainAndCV(1).cache()
    val allItemIDs = allData.map(_.product).distinct().collect()
    val allItemIDsBC = sc.broadcast(allItemIDs)

    val mostListenedAUC = areaUnderCurve(cvData, allItemIDsBC, predictMostListened(sc, trainData))
    println(mostListenedAUC)

    val evaluations =
      for (rank   <- Array(10,  50);
           lambda <- Array(1.0, 0.0001);
           alpha  <- Array(1.0, 40.0))
        yield {
          val model = ALS.trainImplicit(trainData, rank, 10, lambda, alpha)
          val auc = areaUnderCurve(cvData, allItemIDsBC, model.predict)
          ((rank, lambda, alpha), auc)
        }

    evaluations.sortBy(_._2).reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()
  }

  def recommend(sc: SparkContext,
                rawUserArtistData: RDD[String],
                rawArtistData: RDD[String],
                rawArtistAlias: RDD[String]): Unit = {

    val artistAliasBC = sc.broadcast(buildArtistAlias(rawArtistAlias))
    val allData = buildRatings(rawUserArtistData, artistAliasBC).cache()
    val model = ALS.trainImplicit(allData, 50, 10, 1.0, 40.0)
    allData.unpersist()

    val userID = 2093760
    val recommendations = model.recommendProducts(userID, 5)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter(idName =>
      recommendedProductIDs.contains(idName._1)
    ).values.collect().sorted.foreach(println)

    val someUsers = allData.map(_.user).distinct().take(100)
    val someRecommendations = someUsers.map(userID => model.recommendProducts(userID, 5))
    someRecommendations.map(
      recs => recs(0).user + " -> " + recs.map(_.product).mkString(",")
    ).foreach(println)
  }

}