/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.risk

import java.io.File
import java.text.SimpleDateFormat
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import breeze.plot._

import com.github.nscala_time.time.Implicits._

import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.distribution.MultivariateNormalDistribution
import org.apache.commons.math3.random.MersenneTwister
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

import org.joda.time.DateTime

object RunRisk {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("VaR"))
    val (stocksReturns, factorsReturns) = readStocksAndFactors("./")
    plotDistribution(factorsReturns(2))
    plotDistribution(factorsReturns(3))
    val numTrials = 10000000
    val parallelism = 1000
    val baseSeed = 1001L
    val trials = computeTrialReturns(stocksReturns, factorsReturns, sc, baseSeed, numTrials,
      parallelism)
    trials.cache()
    val valueAtRisk = fivePercentVaR(trials)
    val conditionalValueAtRisk = fivePercentCVaR(trials)
    println("VaR 5%: " + valueAtRisk)
    println("CVaR 5%: " + conditionalValueAtRisk)
    val varConfidenceInterval = bootstrappedConfidenceInterval(trials, fivePercentVaR, 100, .05)
    val cvarConfidenceInterval = bootstrappedConfidenceInterval(trials, fivePercentCVaR, 100, .05)
    println("VaR confidence interval: " + varConfidenceInterval)
    println("CVaR confidence interval: " + cvarConfidenceInterval)
    println("Kupiec test p-value: " + kupiecTestPValue(stocksReturns, valueAtRisk, 0.05))
    plotDistribution(trials)
  }

  def computeTrialReturns(
      stocksReturns: Seq[Array[Double]],
      factorsReturns: Seq[Array[Double]],
      sc: SparkContext,
      baseSeed: Long,
      numTrials: Int,
      parallelism: Int): RDD[Double] = {
    val factorMat = factorMatrix(factorsReturns)
    val factorCov = new Covariance(factorMat).getCovarianceMatrix().getData()
    val factorMeans = factorsReturns.map(factor => factor.sum / factor.size).toArray
    val factorFeatures = factorMat.map(featurize)
    val factorWeights = computeFactorWeights(stocksReturns, factorFeatures)

    val bInstruments = sc.broadcast(factorWeights)

    // Generate different seeds so that our simulations don't all end up with the same results
    val seeds = (baseSeed until baseSeed + parallelism)
    val seedRdd = sc.parallelize(seeds, parallelism)

    // Main computation: run simulations and compute aggregate return for each
    seedRdd.flatMap(
      trialReturns(_, numTrials / parallelism, bInstruments.value, factorMeans, factorCov))
  }

  def computeFactorWeights(
      stocksReturns: Seq[Array[Double]],
      factorFeatures: Array[Array[Double]]): Array[Array[Double]] = {
    stocksReturns.map(linearModel(_, factorFeatures)).map(_.estimateRegressionParameters()).toArray
  }

  def featurize(factorReturns: Array[Double]): Array[Double] = {
    val squaredReturns = factorReturns.map(x => math.signum(x) * x * x)
    val squareRootedReturns = factorReturns.map(x => math.signum(x) * math.sqrt(math.abs(x)))
    squaredReturns ++ squareRootedReturns ++ factorReturns
  }

  def readStocksAndFactors(prefix: String): (Seq[Array[Double]], Seq[Array[Double]]) = {
    val start = new DateTime(2009, 10, 23, 0, 0)
    val end = new DateTime(2014, 10, 23, 0, 0)

    val rawStocks = readHistories(new File(prefix + "data/stocks/")).filter(_.size >= 260*5+10)
    val stocks = rawStocks.map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))

    val factorsPrefix = prefix + "data/factors/"
    val factors1 = Array("crudeoil.tsv", "us30yeartreasurybonds.tsv").
      map(x => new File(factorsPrefix + x)).
      map(readInvestingDotComHistory)
    val factors2 = Array("^GSPC.csv", "^IXIC.csv").
      map(x => new File(factorsPrefix + x)).
      map(readYahooHistory)

    val factors = (factors1 ++ factors2).
      map(trimToRegion(_, start, end)).
      map(fillInHistory(_, start, end))

    val stockReturns = stocks.map(twoWeekReturns)
    val factorReturns = factors.map(twoWeekReturns)
    (stockReturns, factorReturns)
  }

  def trialReturns(
      seed: Long,
      numTrials: Int,
      instruments: Seq[Array[Double]],
      factorMeans: Array[Double],
      factorCovariances: Array[Array[Double]]): Seq[Double] = {
    val rand = new MersenneTwister(seed)
    val multivariateNormal = new MultivariateNormalDistribution(rand, factorMeans,
      factorCovariances)

    val trialReturns = new Array[Double](numTrials)
    for (i <- 0 until numTrials) {
      val trialFactorReturns = multivariateNormal.sample()
      val trialFeatures = RunRisk.featurize(trialFactorReturns)
      trialReturns(i) = trialReturn(trialFeatures, instruments)
    }
    trialReturns
  }

  /**
   * Calculate the full return of the portfolio under particular trial conditions.
   */
  def trialReturn(trial: Array[Double], instruments: Seq[Array[Double]]): Double = {
    var totalReturn = 0.0
    for (instrument <- instruments) {
      totalReturn += instrumentTrialReturn(instrument, trial)
    }
    totalReturn / instruments.size
  }

  /**
   * Calculate the return of a particular instrument under particular trial conditions.
   */
  def instrumentTrialReturn(instrument: Array[Double], trial: Array[Double]): Double = {
    var instrumentTrialReturn = instrument(0)
    var i = 0
    while (i < trial.length) {
      instrumentTrialReturn += trial(i) * instrument(i+1)
      i += 1
    }
    instrumentTrialReturn
  }

  def twoWeekReturns(history: Array[(DateTime, Double)]): Array[Double] = {
    history.sliding(10).map { window =>
      val next = window.last._2
      val prev = window.head._2
      (next - prev) / prev
    }.toArray
  }

  def linearModel(instrument: Array[Double], factorMatrix: Array[Array[Double]])
  : OLSMultipleLinearRegression = {
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
      if (cur.tail.nonEmpty && cur.tail.head._1 == curDate) {
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
    val format = new SimpleDateFormat("MMM d, yyyy", Locale.ENGLISH)
    val lines = Source.fromFile(file).getLines().toSeq
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
    val format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)
    val lines = Source.fromFile(file).getLines().toSeq
    lines.tail.map(line => {
      val cols = line.split(',')
      val date = new DateTime(format.parse(cols(0)))
      val value = cols(1).toDouble
      (date, value)
    }).reverse.toArray
  }

  def plotDistribution(samples: Array[Double]): Figure = {
    val min = samples.min
    val max = samples.max
    // Using toList before toArray avoids a Scala bug
    val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
    val densities = KernelDensity.estimate(samples, domain)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
    p.xlabel = "Two Week Return ($)"
    p.ylabel = "Density"
    f
  }

  def plotDistribution(samples: RDD[Double]): Figure = {
    val stats = samples.stats()
    val min = stats.min
    val max = stats.max
    // Using toList before toArray avoids a Scala bug
    val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
    val densities = KernelDensity.estimate(samples, domain)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
    p.xlabel = "Two Week Return ($)"
    p.ylabel = "Density"
    f
  }

  def fivePercentVaR(trials: RDD[Double]): Double = {
    val topLosses = trials.takeOrdered(math.max(trials.count().toInt / 20, 1))
    topLosses.last
  }

  def fivePercentCVaR(trials: RDD[Double]): Double = {
    val topLosses = trials.takeOrdered(math.max(trials.count().toInt / 20, 1))
    topLosses.sum / topLosses.length
  }

  def bootstrappedConfidenceInterval(
      trials: RDD[Double],
      computeStatistic: RDD[Double] => Double,
      numResamples: Int,
      pValue: Double): (Double, Double) = {
    val stats = (0 until numResamples).map { i =>
      val resample = trials.sample(true, 1.0)
      computeStatistic(resample)
    }.sorted
    val lowerIndex = (numResamples * pValue / 2 - 1).toInt
    val upperIndex = math.ceil(numResamples * (1 - pValue / 2)).toInt
    (stats(lowerIndex), stats(upperIndex))
  }

  def countFailures(stocksReturns: Seq[Array[Double]], valueAtRisk: Double): Int = {
    var failures = 0
    for (i <- 0 until stocksReturns(0).size) {
      val loss = stocksReturns.map(_(i)).sum
      if (loss < valueAtRisk) {
        failures += 1
      }
    }
    failures
  }

  def kupiecTestStatistic(total: Int, failures: Int, confidenceLevel: Double): Double = {
    val failureRatio = failures.toDouble / total
    val logNumer = (total - failures) * math.log1p(-confidenceLevel) +
      failures * math.log(confidenceLevel)
    val logDenom = (total - failures) * math.log1p(-failureRatio) +
      failures * math.log(failureRatio)
    -2 * (logNumer - logDenom)
  }

  def kupiecTestPValue(
      stocksReturns: Seq[Array[Double]],
      valueAtRisk: Double,
      confidenceLevel: Double): Double = {
    val failures = countFailures(stocksReturns, valueAtRisk)
    val total = stocksReturns(0).size
    val testStatistic = kupiecTestStatistic(total, failures, confidenceLevel)
    1 - new ChiSquaredDistribution(1.0).cumulativeProbability(testStatistic)
  }

}
