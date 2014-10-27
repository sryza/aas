package com.cloudera.datascience.risk

// https://github.com/scalanlp/breeze/wiki/Quickstart
// http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/random/EmpiricalDistribution.html

import breeze.plot._

import com.github.nscala_time.time.Imports._

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.commons.math3.stat.correlation.Covariance

import scala.io.Source
import scala.collection.mutable.ArrayBuffer

object ComputeFactorWeights {
  val fiveYears = 260 * 5

/*  def plotDistribution(points: Array[Double], numXs: Int, stdDev: Double = 1.0) {
    val lower = points.min
    val upper = points.max
    val xs = (lower until upper by (upper - lower) / numXs).toArray
    val ys = KernelDensity.estimate(points, stdDev, xs)

    val f = Figure()
    val p = f.subplot(0)
    p += plot(xs, ys)
    f.refresh()
  }*/

  def twoWeekReturns(history: Array[(DateTime, Double)]): Array[Double] = {
    history.sliding(10).map(window => window.last._2 - window.head._2).toArray
  }

  def linearModel(instrument: Array[Double], factorMatrix: Array[Array[Double]]): OLSMultipleLinearRegression = {
    val regression = new OLSMultipleLinearRegression()
    regression.newSampleData(instrument, factorMatrix)
    regression
  }

  def factorMatrix(histories: Seq[Array[Double]]): Array[Array[Double]] = {
    val mat = new Array[Array[Double]](histories.head.length)
    for (i <- 0 until histories.head.length) {
      mat(i) = histories.map(_(i)).toArray
    }
    mat
  }

  def readHistories(dir: File): Seq[Array[(DateTime, Double)]] = {
    val files = dir.listFiles()
    files.flatMap(file => {
      try {
        Some(readYahooHistory(file))
      } catch {
        case e: Exception => None
      }
    })
  }

  def trimToRegion(history: Array[(DateTime, Double)], start: DateTime, end: DateTime)
    : Array[(DateTime, Double)] = {
    var trimmed = history.dropWhile(_._1 < start).takeWhile(_._1 <= end)
    if (trimmed.head._1 != start) {
      trimmed = Array((start, trimmed.head._2)) ++ trimmed
    }
    if (trimmed.last._1 != end) {
      trimmed = trimmed ++ Array((end, trimmed.last._2))
    }
    trimmed
  }

  /**
   * Given a timeseries of values of an instruments, returns a timeseries between the given
   * start and end dates with all missing weekdays imputed. Values are imputed as the value on the
   * most recent previous given day.
   */
  def fillInHistory(history: Array[(DateTime, Double)], start: DateTime, end: DateTime)
    : Array[(DateTime, Double)] = {
    var cur = history
    val filled = new ArrayBuffer[(DateTime, Double)]()
    var curDate = start
    while (curDate < end) {
      if (!cur.tail.isEmpty && cur.tail.head._1 == curDate) {
        cur = cur.tail
      }

      filled += ((curDate, cur.head._2))

      curDate += 1.days
      // Skip weekends
      if (curDate.dayOfWeek().get > 5) curDate += 2.days
    }
    filled.toArray
  }

  def readInvestingDotComHistory(file: File): Array[(DateTime, Double)] = {
    val format = new SimpleDateFormat("MMM d, yyyy")
    val lines = Source.fromFile(file).getLines.toSeq
    lines.map(line => {
      val cols = line.split('\t')
      val date = new DateTime(format.parse(cols(0)))
      val value = cols(1).toDouble
      (date, value)
    }).reverse.toArray
  }

  /**
   * Reads a history in the Yahoo format
   */
  def readYahooHistory(file: File): Array[(DateTime, Double)] = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val lines = Source.fromFile(file).getLines.toSeq
    lines.tail.map(line => {
      val cols = line.split(',')
      val date = new DateTime(format.parse(cols(0)))
      val value = cols(1).toDouble
      (date, value)
    }).reverse.toArray
  }

  def plotDistribution(samples: Array[Double], bandwidth: Double = -1) {
    val min = samples.min
    val max = samples.max
    // Using toList before toArray avoids a Scala bug
    val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
    val densities = KernelDensity.estimate(samples, domain, bandwidth)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
  }
}