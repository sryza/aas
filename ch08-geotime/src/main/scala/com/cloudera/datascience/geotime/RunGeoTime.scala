/*
 * Copyright 2015 and onwards Sanford Ryza, Juliet Hougland, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.geotime

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import com.esri.core.geometry.Point
import spray.json._

import com.cloudera.datascience.geotime.GeoJsonProtocol._

class RichRow(row: Row) {
  def getAs[T](field: String): Option[T] =
    if (row.isNullAt(row.fieldIndex(field))) None else Some(row.getAs[T](field))
}

case class Trip(
  license: String,
  pickupTime: Long,
  dropoffTime: Long,
  pickupX: Double,
  pickupY: Double,
  dropoffX: Double,
  dropoffY: Double)

object RunGeoTime extends Serializable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val taxiRaw = spark.read.option("header", "true").csv("taxidata")
    val taxiParsed = taxiRaw.rdd.map(safe(parse))
    val taxiGood = taxiParsed.collect({
      case t if t.isLeft => t.left.get
    }).toDS
    taxiGood.cache()

    val hours = (pickup: Long, dropoff: Long) => {
      TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
    }
    val hoursUDF = udf(hours)

    taxiGood.groupBy(hoursUDF($"pickupTime", $"dropoffTime").as("h")).count().sort("h").show()

    // register the UDF, use it in a where clause
    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where("hours(pickupTime, dropoffTime) BETWEEN 0 AND 3")

    val geojson = scala.io.Source.fromURL(this.getClass.getResource("/nyc-boroughs.geojson")).mkString

    val features = geojson.parseJson.convertTo[FeatureCollection]
    val areaSortedFeatures = features.sortBy { f => 
      val borough = f("boroughCode").convertTo[Int]
      (borough, -f.geometry.area2D())
    }

    val bFeatures = spark.sparkContext.broadcast(areaSortedFeatures)

    val borough = (x: Double, y: Double) => {
      val feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(new Point(x, y))
      })
      feature.map(f => {
        f("borough").convertTo[String]
      }).getOrElse("NA")
    }
    val boroughUDF = udf(borough)

    taxiClean.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()
    val taxiDone = taxiClean.where("dropoffX != 0 and dropoffY != 0 and pickupX != 0 and pickupY != 0")
    taxiDone.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()

    taxiGood.unpersist()

    val sessions = taxiDone.
        repartition($"license").
        sortWithinPartitions($"license", $"pickupTime").
        cache()
    def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
      val b = borough(t1.dropoffX, t1.dropoffY)
      val d = (t2.pickupTime - t1.dropoffTime) / 1000
      (b, d)
    }

    val boroughDurations: DataFrame =
      sessions.mapPartitions(trips => {
        val iter: Iterator[Seq[Trip]] = trips.sliding(2)
        val viter = iter.filter(_.size == 2).filter(p => p(0).license == p(1).license)
        viter.map(p => boroughDuration(p(0), p(1)))
      }).toDF("borough", "seconds")
    boroughDurations.
      where("seconds > 0").
      groupBy("borough").
      agg(avg("seconds"), stddev("seconds")).
      show()

    boroughDurations.unpersist()
  }

  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }

  def parseTaxiTime(datetime: Option[String]): Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    datetime.map(dt => formatter.parse(dt).getTime).getOrElse(0L)
  }

  def parse(line: Row): Trip = {
    val fields = new RichRow(line)
    val license = fields.getAs[String]("hack_license").orNull
    val pickupTime = parseTaxiTime(fields.getAs[String]("pickup_datetime"))
    val dropoffTime = parseTaxiTime(fields.getAs[String]("dropoff_datetime"))
    val pickupX = fields.getAs[String]("pickup_longitude").map(_.toDouble).getOrElse(0.0)
    val pickupY = fields.getAs[String]("pickup_latitude").map(_.toDouble).getOrElse(0.0)
    val dropoffX = fields.getAs[String]("dropoff_longitude").map(_.toDouble).getOrElse(0.0)
    val dropoffY = fields.getAs[String]("dropoff_latitude").map(_.toDouble).getOrElse(0.0)
    Trip(license, pickupTime, dropoffTime, pickupX, pickupY, dropoffX, dropoffY)
  }
}
