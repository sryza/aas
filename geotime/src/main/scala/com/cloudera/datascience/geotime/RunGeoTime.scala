/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.geotime

import com.cloudera.science.geojson._
import com.cloudera.science.geojson.GeoJsonProtocol._
import com.esri.core.geometry.Point
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.StatCounter
import org.apache.spark.SparkContext._
import org.joda.time.{DateTime, Duration}
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer
import spray.json._

case class Trip(
  pickup_time: DateTime,
  dropoff_time: DateTime,
  pickup_loc: Point,
  dropoff_loc: Point)

object RunGeoTime extends Serializable {

  val formatter = new SparkDateTimeFormatter(
    "yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("GeoTime"))
    val taxiraw = sc.textFile("taxidata")

    val safeParse = safe(parse)
    val taxiparsed = taxiraw.map(safeParse)
    taxiparsed.cache()

    val taxibad = taxiparsed.collect {
      case t if t.isRight => t.right.get
    }
    taxibad.collect().foreach(println)

    val taxigood = taxiparsed.collect {
      case t if t.isLeft => t.left.get
    }
    taxigood.cache()

    taxigood.values.map(t => hours(t)).
    countByValue().
    toList.
    sorted.
    foreach(println)

    val taxiclean = taxigood.filter {
      case (lic, trip) => {
        val hrs = hours(trip)
        0 <= hrs && hrs < 3
      }
    }

    val geojson = scala.io.Source.
    fromURL(getClass.getResource("/nyc-boroughs.geojson")).
    mkString

    val jso = geojson.parseJson
    val features = jso.convertTo[FeatureCollection]
    val frs = features.sortBy(f => {
      val b = f("boroughCode").convertTo[Int]
      (b, -f.geometry.area2D())
    })

    val bfrs = sc.broadcast(frs)
    val borough = new Function[Trip, Option[String]] with Serializable {
      def apply(trip: Trip): Option[String] = {
        val optf = bfrs.value.find(f => {
          f.geometry.contains(trip.dropoff_loc)
        })
        optf.map(f => {
          f("borough").convertTo[String]
        })
      }
    }
    taxiclean.values.
    map(t => borough(t)).
    countByValue().
    foreach(println)

    val taxidone = taxiclean.filter {
      case (lic, trip) => !hasZero(trip)
    }.cache()
    taxidone.values.
    map(t => borough(t)).
    countByValue().
    foreach(println)

    val presess = taxidone.map {
      case (lic, trip) => {
        ((lic, trip.pickup_time.getMillis), trip)
      }
    }
    val fkp = new FirstKeyPartitioner[String, Long](30)
    val postsess = presess.repartitionAndSortWithinPartitions(fkp)
    val sessions = postsess.mapPartitions(iter => toSessions(iter))
    sessions.cache()

    val boroughDuration = new Function2[Trip, Trip, (Option[String], Duration)]
        with Serializable {
      def apply(t1: Trip, t2: Trip) = {
        val b = borough(t1)
        val d = new Duration(
          t1.dropoff_time,
          t2.pickup_time)
        (b, d)
      }
    }

    val bdrdd = sessions.values.
    flatMap(trips => {
      val iter = trips.sliding(2)
      val viter = iter.filter(_.size == 2)
      viter.map(p => boroughDuration(p(0), p(1)))
    }).cache()

    bdrdd.values.map(_.getStandardHours).
    countByValue().
    toList.
    sorted.
    foreach(println)

    bdrdd.filter {
      case (b, d) => d.getMillis >= 0
    }.mapValues(d => {
      val s = new StatCounter()
      s.merge(d.getStandardSeconds)
    }).
    reduceByKey((a, b) => a.merge(b)).
    collect().
    foreach(println)
  }

  def hasZero(trip: Trip) = {
    val zero = new Point(0.0, 0.0)
    (zero.equals(trip.pickup_loc) ||
     zero.equals(trip.dropoff_loc))
  }

  def point(longitude: String, latitude: String) = {
    new Point(longitude.toDouble, latitude.toDouble)
  }

  def parse(line: String): (String, Trip) = {
    val p = line.split(",")
    val license = p(1)
    val pickup_tm = formatter.parse(p(5))
    val dropoff_tm = formatter.parse(p(6))
    val pickup_loc = point(p(10), p(11))
    val dropoff_loc = point(p(12), p(13))

    val trip = Trip(pickup_tm, dropoff_tm,
      pickup_loc, dropoff_loc)
    (license, trip)
  }

  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          return Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }

  def hours(trip: Trip) = {
    val d = new Duration(
      trip.pickup_time,
      trip.dropoff_time)
    d.getStandardHours
  }

  def split(t1: Trip, t2: Trip) = {
    val p1 = t1.pickup_time
    val p2 = t2.pickup_time
    val d = new Duration(p1, p2)
    d.getStandardHours >= 4
  }

  def toSessions(it: Iterator[((String, Long), Trip)]):
      Iterator[(String, List[Trip])] = {
    val res = List[(String, ArrayBuffer[Trip])]()
    it.foldLeft(res)((list, next) => list match {
      case Nil => {
        val ((lic, ts), trip) = next
        List((lic, ArrayBuffer(trip)))
      }
      case cur :: rest => {
        val (curLic, trips) = cur
        val ((lic, ts), trip) = next
        if (!lic.equals(curLic) || split(trips.last, trip)) {
          (lic, ArrayBuffer(trip)) :: list
        } else {
          trips.append(trip)
          list
        }
      }
    }).map { case (lic, buf) => (lic, buf.toList) }.iterator
  }
}

class SparkDateTimeFormatter(val fmt: String) extends
  Serializable {
  @transient var formatter: DateTimeFormatter = null

  private def getFormatter(): DateTimeFormatter = {
    if (formatter == null) {
      formatter = DateTimeFormat.forPattern(fmt)
    }
    formatter
  }

  def parse(str: String): DateTime = {
    getFormatter().parseDateTime(str)
  }

  def print(dt: DateTime): String = {
    getFormatter().print(dt)
  }
}

class FirstKeyPartitioner[K1, K2](partitions: Int) extends
  Partitioner {
  val delegate = new HashPartitioner(partitions)
  override def numPartitions = delegate.numPartitions
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(K1, K2)]
    delegate.getPartition(k._1)
  }
}

