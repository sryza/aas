/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.recommender

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object RunRecommender {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      config("spark.sql.crossJoin.enabled", true).
      getOrCreate()

    val base = "hdfs:///user/ds/"
    val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
    val rawArtistData = spark.read.textFile(base + "artist_data.txt")
    val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")

    preparation(spark, rawUserArtistData, rawArtistData, rawArtistAlias)
    model(spark, rawUserArtistData, rawArtistData, rawArtistAlias)
    evaluate(spark, rawUserArtistData, rawArtistAlias)
    recommend(spark, rawUserArtistData, rawArtistData, rawArtistAlias)
  }

  def preparation(
      spark: SparkSession,
      rawUserArtistData: Dataset[String],
      rawArtistData: Dataset[String],
      rawArtistAlias: Dataset[String]): Unit = {

    import spark.implicits._

    val userArtistDF = rawUserArtistData.map { line =>
      val Array(user, artist, _*) = line.split(' ')
      (user.toInt, artist.toInt)
    }.toDF("user", "artist")

    userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()

    val artistByID = buildArtistByID(spark, rawArtistData)
    val artistAlias = buildArtistAlias(spark, rawArtistAlias)

    val (badID, goodID) = artistAlias.head
    val badArtist = artistByID.filter(col("id") === badID).select("name").as[String].first()
    val goodArtist = artistByID.filter(col("id") === goodID).select("name").as[String].first()
    println(s"$badArtist -> $goodArtist")
  }

  def model(
      spark: SparkSession,
      rawUserArtistData: Dataset[String],
      rawArtistData: Dataset[String],
      rawArtistAlias: Dataset[String]): Unit = {

    import spark.implicits._

    val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(spark, rawArtistAlias))

    val trainData = buildCounts(spark, rawUserArtistData, bArtistAlias).cache()

    val model = new ALS().
      setImplicitPrefs(true).
      setRank(10).setRegParam(0.01).setAlpha(1.0).setMaxIter(5).
      setUserCol("user").setItemCol("artist").
      setRatingCol("count").setPredictionCol("prediction").
      fit(trainData)

    trainData.unpersist()

    model.userFactors.show(1)

    val userID = 2093760
    val topRecommendations = makeRecommendations(model, userID, 5)
    topRecommendations.show()

    val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()
    val existingArtistIDs = trainData.
      filter(col("user") === userID).
      select("artist").as[Int].collect()

    val artistByID = buildArtistByID(spark, rawArtistData)

    artistByID.join(spark.createDataset(existingArtistIDs).toDF("id"), "id").
      select("name").show()
    artistByID.join(spark.createDataset(recommendedArtistIDs).toDF("id"), "id").
      select("name").show()

    unpersist(model)
  }

  def evaluate(
      spark: SparkSession,
      rawUserArtistData: Dataset[String],
      rawArtistAlias: Dataset[String]): Unit = {

    import spark.implicits._

    val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(spark, rawArtistAlias))

    val allData = buildCounts(spark, rawUserArtistData, bArtistAlias)
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val allArtistIDs = allData.select("artist").as[Int].distinct().collect()
    val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

    val mostListenedAUC = areaUnderCurve(
      spark, cvData, bAllArtistIDs, predictMostListened(spark, trainData))
    println(mostListenedAUC)

    val evaluations =
      for (rank   <- Seq(10,  50);
           lambda <- Seq(1.0, 0.0001);
           alpha  <- Seq(1.0, 40.0))
      yield {
        val model = new ALS().
          setImplicitPrefs(true).
          setRank(rank).setRegParam(lambda).setAlpha(alpha).setMaxIter(10).
          setUserCol("user").setItemCol("artist").
          setRatingCol("count").setPredictionCol("prediction").
          fit(trainData)
        val auc = areaUnderCurve(spark, cvData, bAllArtistIDs, model.transform)
        unpersist(model)
        ((rank, lambda, alpha), auc)
      }

    evaluations.sortBy(_._2).reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()
  }

  def recommend(
      spark: SparkSession,
      rawUserArtistData: Dataset[String],
      rawArtistData: Dataset[String],
      rawArtistAlias: Dataset[String]): Unit = {

    import spark.implicits._

    val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(spark, rawArtistAlias))
    val allData = buildCounts(spark, rawUserArtistData, bArtistAlias).cache()
    val model = new ALS().
      setImplicitPrefs(true).
      setRank(50).setRegParam(1.0).setAlpha(40.0).setMaxIter(10).
      setUserCol("user").setItemCol("artist").
      setRatingCol("count").setPredictionCol("prediction").
      fit(allData)
    allData.unpersist()

    val userID = 2093760
    val topRecommendations = makeRecommendations(model, userID, 5)

    val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()
    val artistByID = buildArtistByID(spark, rawArtistData)
    artistByID.join(spark.createDataset(recommendedArtistIDs).toDF("id"), "id").
      select("name").show()

    unpersist(model)
  }

  def buildArtistByID(spark: SparkSession, rawArtistData: Dataset[String]): DataFrame = {
    import spark.implicits._
    rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case _: NumberFormatException => None
        }
      }
    }.toDF("id", "name")
  }

  def buildArtistAlias(spark: SparkSession, rawArtistAlias: Dataset[String]): Map[Int,Int] = {
    import spark.implicits._
    rawArtistAlias.flatMap { line =>
      val Array(artist, alias) = line.split('\t')
      if (artist.isEmpty) {
        None
      } else {
        Some((artist.toInt, alias.toInt))
      }
    }.collect().toMap
  }

  def buildCounts(
      spark: SparkSession,
      rawUserArtistData: Dataset[String],
      bArtistAlias: Broadcast[Map[Int,Int]]): DataFrame = {
    import spark.implicits._
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      (userID, finalArtistID, count)
    }.toDF("user", "artist", "count")
  }

  def makeRecommendations(model: ALSModel, userID: Int, howMany: Int): DataFrame = {
    val toRecommend = model.itemFactors.
      withColumnRenamed("id", "artist").
      withColumn("user", lit(userID)).
      select("user", "artist")
    model.transform(toRecommend).
      select("artist", "prediction").
      orderBy(col("prediction").desc).
      limit(howMany)
  }


  def areaUnderCurve(
      spark: SparkSession,
      positiveData: DataFrame,
      bAllArtistIDs: Broadcast[Array[Int]],
      predictFunction: (DataFrame => DataFrame)): Double = {

    import spark.implicits._

    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive".
    // Make predictions for each of them, including a numeric score
    val positivePredictions = predictFunction(positiveData).
      withColumnRenamed("prediction", "positivePrediction")

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other artists, excluding those that are "positive" for the user.
    val negativeData = positiveData.select("user", "artist").
      map(row => (row.getInt(0), row.getInt(1))).
      groupByKey { case (user, _) => user }.
      flatMapGroups { case (userID, userIDAndPosArtistIDs) =>
        val random = new Random()
        val posItemIDSet = userIDAndPosArtistIDs.map { case (_, artist) => artist }.toSet
        val negative = new ArrayBuffer[Int]()
        val allArtistIDs = bAllArtistIDs.value
        var i = 0
        while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
          val artistID = allArtistIDs(random.nextInt(allArtistIDs.length))
          if (!posItemIDSet.contains(artistID)) {
            negative += artistID
          }
          i += 1
        }
        negative.map(itemID => (userID, itemID))
      }.toDF("user", "artist")

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeData).
      withColumnRenamed("prediction", "negativePrediction")

    val joinedPredictions = positivePredictions.join(negativePredictions, "user").
      select("user", "positivePrediction", "negativePrediction").cache()

    val allCounts = joinedPredictions.
      groupBy("user").agg(count("user").as("total")).
      select("user", "total")
    val correctCounts = joinedPredictions.
      filter(col("positivePrediction") > col("negativePrediction")).
      groupBy("user").agg(count("user").as("correct")).
      select("user", "correct")

    val meanAUC = allCounts.join(correctCounts, "user").
      select(col("user"), (col("correct") / col("total")).as("auc")).
      agg(mean("auc")).
      as[Double].first()

    joinedPredictions.unpersist()

    meanAUC
  }

  def predictMostListened(spark: SparkSession, train: DataFrame)(allData: DataFrame): DataFrame = {
    val listenCounts = train.groupBy("artist").agg(sum("count").as("prediction"))
    allData.join(listenCounts, "artist").select("user", "artist", "prediction")
  }

  def unpersist(model: ALSModel): Unit = {
    // At the moment, it's necessary to manually unpersist the DataFrames inside the model
    // when done with it in order to make sure they are promptly uncached
    model.userFactors.unpersist()
    model.itemFactors.unpersist()
  }

}