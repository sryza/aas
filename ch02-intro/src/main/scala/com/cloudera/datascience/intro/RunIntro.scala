/*
 * Copyright 2015 and onwards Sanford Ryza, Juliet Hougland, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.intro

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._ // for lit()

case class MatchData(
  id_1: Int,
  id_2: Int,
  cmp_fname_c1: Option[Double],
  cmp_fname_c2: Option[Double],
  cmp_lname_c1: Option[Double],
  cmp_lname_c2: Option[Double],
  cmp_sex: Option[Int],
  cmp_bd: Option[Int],
  cmp_bm: Option[Int],
  cmp_by: Option[Int],
  cmp_plz: Option[Int],
  is_match: Boolean
)

case class Scored(score: Double, is_match: Boolean)

object RunIntro extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Intro")
      .getOrCreate
    import spark.implicits._
 
    val preview = spark.read.csv("hdfs:///user/ds/linkage")
    preview.show()
    preview.schema.foreach(println)

    val parsed = spark.read
      .option("header", "true")
      .option("nullValue", "?")
      .option("inferSchema", "true")
      .csv("hdfs:///user/ds/linkage")
      .cache()
    parsed.show()
    val schema = parsed.schema
    schema.foreach(println)

    parsed.count()
    parsed.groupBy("is_match").count().show()

    parsed.createOrReplaceTempView("linkage")
    spark.sql("""
      SELECT is_match, COUNT(*) cnt
      FROM linkage
      GROUP BY is_match
      ORDER BY cnt DESC
    """).show()

    val summary = parsed.describe()
    summary.show()
    summary.select("summary", "cmp_fname_c1", "cmp_lname_c1").show()
    summary.select("summary", "cmp_fname_c2", "cmp_lname_c2").show()

    val matches = parsed.filter("is_match = true")
    val misses = parsed.filter($"is_match" === lit(false))
    val matchSummary = matches.describe()
    val missSummary = misses.describe()

    val matchSummaryLong = longForm(matchSummary)
    val missSummaryLong = longForm(missSummary)
    matchSummaryLong.createOrReplaceTempView("match_long")
    missSummaryLong.createOrReplaceTempView("miss_long")
    spark.sql("""
      SELECT a.field, a.count + b.count total, a.mean - b.mean delta
      FROM match_long a INNER JOIN miss_long b ON a.field = b.field
      ORDER BY delta DESC, total DESC
    """).show()

    val scoredDF = parsed.map { row =>
      (scoreRow(row), row.getAs[Boolean]("is_match"))
    }.toDF("score", "is_match")

    crossTabs(scoredDF).show()

    val matchData = parsed.as[MatchData]
    val scoredDS = matchData.map(md => {
      Scored(scoreMatchData(md), md.is_match)
    })
    crossTabs(scoredDS).show()
  }

  def crossTabs(scored: Dataset[_]): DataFrame = {
    scored.
      selectExpr("score >= 4.0 as above", "is_match").
      groupBy("above").
      pivot("is_match", Seq("true", "false")).
      count()
  }

  def getDoubleOrZero(row: Row, idx: Int): Double = {
    if (row.isNullAt(idx)) 0.0 else row.get(idx).toString.toDouble
  }

  def scoreRow(row: Row): Double = {
    Seq("cmp_plz", "cmp_by", "cmp_bd", "cmp_lname_c1", "cmp_bm").map { f =>
      getDoubleOrZero(row, row.fieldIndex(f))
    }.sum
  }

  def scoreMatchData(md: MatchData): Double = {
    val oid = (oi: Option[Int]) => oi.map(_.toDouble).getOrElse(0.0)
    oid(md.cmp_plz) + oid(md.cmp_by) + oid(md.cmp_bd) +
    md.cmp_lname_c1.getOrElse(0.0) + oid(md.cmp_bm)
  }

  def longForm(desc: DataFrame): DataFrame = {
    import desc.sparkSession.implicits._ // For toDF RDD -> DataFrame conversion

    val schema = desc.schema
    desc.rdd.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => (schema(i).name, (metric, row.getString(i).toDouble)))
    })
    .groupByKey()
    .map(kv => {
      val field = kv._1
      val m = kv._2.toMap
      (field, m("count"), m("mean"), m("stddev"), m("min"), m("max"))
    })
    .toDF("field", "count", "mean", "stddev", "min", "max")
  }
}
