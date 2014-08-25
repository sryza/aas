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

import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object RunRecommender {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Recommender"))
    val rawUserArtistData = sc.textFile("/user/spark/user_artist_data.txt", 120)
    val rawArtistData = sc.textFile("/user/spark/artist_data.txt")
    val rawArtistAlias = sc.textFile("/user/spark/artist_alias.txt")

    preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
    model(sc, rawUserArtistData, rawArtistAlias)
  }

  def preparation(rawUserArtistData: RDD[String],
                  rawArtistData: RDD[String],
                  rawArtistAlias: RDD[String]) = {

    val userIDStats = rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
    val itemIDStats = rawUserArtistData.map(_.split(' ')(1).toDouble).stats()
    println(userIDStats)
    println(itemIDStats)

    val artistByID = rawArtistData.flatMap { line =>
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

    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()

    val (badID, goodID) = artistAlias.head
    println(artistByID.lookup(badID) + " -> " + artistByID.lookup(goodID))
  }

  def model(sc: SparkContext, rawUserArtistData: RDD[String], rawArtistAlias: RDD[String]): Unit = {

    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()

    val artistAliasBroadcast = sc.broadcast(artistAlias)

    val implicitFeedback = rawUserArtistData.map { line =>
      val tokens = line.split(' ')
      val userID = tokens(0).toInt
      val originalArtistID = tokens(1).toInt
      val count = tokens(2).toInt
      val artistID = artistAliasBroadcast.value.getOrElse(originalArtistID, originalArtistID)
      Rating(userID, artistID, count)
    }.cache()

    val model = ALS.trainImplicit(implicitFeedback, 10, 5, 0.01, 1.0)

    implicitFeedback.unpersist()

    model.userFeatures.mapValues(java.util.Arrays.toString).take(3).foreach(println)
  }

}