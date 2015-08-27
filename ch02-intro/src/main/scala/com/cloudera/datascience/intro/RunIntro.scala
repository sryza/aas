/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.intro

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

case class MatchData(id1: Int, id2: Int,
  scores: Array[Double], matched: Boolean)
case class Scored(md: MatchData, score: Double)

object RunIntro extends Serializable {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Intro"))
   
    val rawblocks = sc.textFile("hdfs:///user/ds/linkage")
    def isHeader(line: String) = line.contains("id_1")
    
    val noheader = rawblocks.filter(x => !isHeader(x))
    def toDouble(s: String) = {
     if ("?".equals(s)) Double.NaN else s.toDouble
    }

    def parse(line: String) = {
      val pieces = line.split(',')
      val id1 = pieces(0).toInt
      val id2 = pieces(1).toInt
      val scores = pieces.slice(2, 11).map(toDouble)
      val matched = pieces(11).toBoolean
      MatchData(id1, id2, scores, matched)
    }

    val parsed = noheader.map(line => parse(line))
    parsed.cache()

    val matchCounts = parsed.map(md => md.matched).countByValue()
    val matchCountsSeq = matchCounts.toSeq
    matchCountsSeq.sortBy(_._2).reverse.foreach(println)

    val stats = (0 until 9).map(i => {
      parsed.map(_.scores(i)).filter(!_.isNaN).stats()
    })
    stats.foreach(println)

    val nasRDD = parsed.map(md => {
      md.scores.map(d => NAStatCounter(d))
    })
    val reduced = nasRDD.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
    reduced.foreach(println)

    val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
    val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))
    statsm.zip(statsn).map { case(m, n) =>
      (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }.foreach(println)

    def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
    val ct = parsed.map(md => {
      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)
    })

    ct.filter(s => s.score >= 4.0).
      map(s => s.md.matched).countByValue().foreach(println)
    ct.filter(s => s.score >= 2.0).
      map(s => s.md.matched).countByValue().foreach(println)
  }

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
      val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => {
        nas.zip(arr).foreach { case (n, d) => n.add(d) }
      })
      Iterator(nas)
    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
  }
}

class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if (x.isNaN) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }

  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString: String = {
    "stats: " + stats.toString + " NaN: " + missing
  }
}

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}
