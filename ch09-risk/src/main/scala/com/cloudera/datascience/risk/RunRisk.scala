/*
 * Copyright 2015 and onwards Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.risk

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions
import org.apache.spark.util.StatCounter
import breeze.plot._
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.distribution.MultivariateNormalDistribution
import org.apache.commons.math3.random.MersenneTwister
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression


object RunRisk {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val runRisk = new RunRisk(spark)

    val (stocksReturns, factorsReturns) = runRisk.readStocksAndFactors()
    runRisk.plotDistribution(factorsReturns(2))
    runRisk.plotDistribution(factorsReturns(3))

    val numTrials = 10000000
    val parallelism = 1000
    val baseSeed = 1001L
    val trials = runRisk.computeTrialReturns(stocksReturns, factorsReturns, baseSeed, numTrials,
      parallelism)
    trials.cache()
    val valueAtRisk = runRisk.fivePercentVaR(trials)
    val conditionalValueAtRisk = runRisk.fivePercentCVaR(trials)
    println("VaR 5%: " + valueAtRisk)
    println("CVaR 5%: " + conditionalValueAtRisk)
    val varConfidenceInterval = runRisk.bootstrappedConfidenceInterval(trials,
      runRisk.fivePercentVaR, 100, .05)
    val cvarConfidenceInterval = runRisk.bootstrappedConfidenceInterval(trials,
      runRisk.fivePercentCVaR, 100, .05)
    println("VaR confidence interval: " + varConfidenceInterval)
    println("CVaR confidence interval: " + cvarConfidenceInterval)
    println("Kupiec test p-value: " + runRisk.kupiecTestPValue(stocksReturns, valueAtRisk, 0.05))
    runRisk.plotDistribution(trials)
  }
}

class RunRisk(private val spark: SparkSession) {
  import spark.implicits._

  /**
   * Reads a history in the Google format
   */
  def readGoogleHistory(file: File): Array[(LocalDate, Double)] = {
    val formatter = DateTimeFormatter.ofPattern("d-MMM-yy")
    val lines = scala.io.Source.fromFile(file).getLines().toSeq
    lines.tail.map { line =>
      val cols = line.split(',')
      val date = LocalDate.parse(cols(0), formatter)
      val value = cols(4).toDouble
      (date, value)
    }.reverse.toArray
  }

  def trimToRegion(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate)
    : Array[(LocalDate, Double)] = {
    var trimmed = history.dropWhile(_._1.isBefore(start)).
      takeWhile(x => x._1.isBefore(end) || x._1.isEqual(end))
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
  def fillInHistory(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate)
    : Array[(LocalDate, Double)] = {
    var cur = history
    val filled = new ArrayBuffer[(LocalDate, Double)]()
    var curDate = start
    while (curDate.isBefore(end)) {
      if (cur.tail.nonEmpty && cur.tail.head._1 == curDate) {
        cur = cur.tail
      }

      filled += ((curDate, cur.head._2))

      curDate = curDate.plusDays(1)
      // Skip weekends
      if (curDate.getDayOfWeek.getValue > 5) {
        curDate = curDate.plusDays(2)
      }
    }
    filled.toArray
  }

  def twoWeekReturns(history: Array[(LocalDate, Double)]): Array[Double] = {
    history.sliding(10).map { window =>
      val next = window.last._2
      val prev = window.head._2
      (next - prev) / prev
    }.toArray
  }

  def readStocksAndFactors(): (Seq[Array[Double]], Seq[Array[Double]]) = {
    val start = LocalDate.of(2009, 10, 23)
    val end = LocalDate.of(2014, 10, 23)

    val stocksDir = new File("stocks/")
    val files = stocksDir.listFiles()
    val allStocks = files.iterator.flatMap { file =>
      try {
        Some(readGoogleHistory(file))
      } catch {
        case e: Exception => None
      }
    }
    val rawStocks = allStocks.filter(_.size >= 260 * 5 + 10)

    val factorsPrefix = "factors/"
    val rawFactors = Array("NYSEARCA%3AGLD.csv", "NASDAQ%3ATLT.csv", "NYSEARCA%3ACRED.csv").
      map(x => new File(factorsPrefix + x)).
      map(readGoogleHistory)

    val stocks = rawStocks.map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))

    val factors = rawFactors.
      map(trimToRegion(_, start, end)).
      map(fillInHistory(_, start, end))

    val stocksReturns = stocks.map(twoWeekReturns).toArray.toSeq
    val factorsReturns = factors.map(twoWeekReturns)
    (stocksReturns, factorsReturns)
  }

  def factorMatrix(histories: Seq[Array[Double]]): Array[Array[Double]] = {
    val mat = new Array[Array[Double]](histories.head.length)
    for (i <- histories.head.indices) {
      mat(i) = histories.map(_(i)).toArray
    }
    mat
  }

  def featurize(factorReturns: Array[Double]): Array[Double] = {
    val squaredReturns = factorReturns.map(x => math.signum(x) * x * x)
    val squareRootedReturns = factorReturns.map(x => math.signum(x) * math.sqrt(math.abs(x)))
    squaredReturns ++ squareRootedReturns ++ factorReturns
  }

  def linearModel(instrument: Array[Double], factorMatrix: Array[Array[Double]])
    : OLSMultipleLinearRegression = {
    val regression = new OLSMultipleLinearRegression()
    regression.newSampleData(instrument, factorMatrix)
    regression
  }

  def computeFactorWeights(
    stocksReturns: Seq[Array[Double]],
    factorFeatures: Array[Array[Double]]): Array[Array[Double]] = {
    stocksReturns.map(linearModel(_, factorFeatures)).map(_.estimateRegressionParameters()).toArray
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
      val trialFeatures = featurize(trialFactorReturns)
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

  def plotDistribution(samples: Array[Double]): Figure = {
    val min = samples.min
    val max = samples.max
    val stddev = new StatCounter(samples).stdev
    val bandwidth = 1.06 * stddev * math.pow(samples.size, -.2)

    // Using toList before toArray avoids a Scala bug
    val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
    val kd = new KernelDensity().
      setSample(samples.toSeq.toDS.rdd).
      setBandwidth(bandwidth)
    val densities = kd.estimate(domain)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
    p.xlabel = "Two Week Return ($)"
    p.ylabel = "Density"
    f
  }

  def plotDistribution(samples: Dataset[Double]): Figure = {
    val (min, max, count, stddev) = samples.agg(
      functions.min($"value"),
      functions.max($"value"),
      functions.count($"value"),
      functions.stddev_pop($"value")
    ).as[(Double, Double, Long, Double)].first()
    val bandwidth = 1.06 * stddev * math.pow(count, -.2)

    // Using toList before toArray avoids a Scala bug
    val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
    val kd = new KernelDensity().
      setSample(samples.rdd).
      setBandwidth(bandwidth)
    val densities = kd.estimate(domain)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
    p.xlabel = "Two Week Return ($)"
    p.ylabel = "Density"
    f
  }

  def fivePercentVaR(trials: Dataset[Double]): Double = {
    val quantiles = trials.stat.approxQuantile("value", Array(0.05), 0.0)
    quantiles.head
  }

  def fivePercentCVaR(trials: Dataset[Double]): Double = {
    val topLosses = trials.orderBy("value").limit(math.max(trials.count().toInt / 20, 1))
    topLosses.agg("value" -> "avg").first()(0).asInstanceOf[Double]
  }

  def bootstrappedConfidenceInterval(
      trials: Dataset[Double],
      computeStatistic: Dataset[Double] => Double,
      numResamples: Int,
      probability: Double): (Double, Double) = {
    val stats = (0 until numResamples).map { i =>
      val resample = trials.sample(true, 1.0)
      computeStatistic(resample)
    }.sorted
    val lowerIndex = (numResamples * probability / 2 - 1).toInt
    val upperIndex = math.ceil(numResamples * (1 - probability / 2)).toInt
    (stats(lowerIndex), stats(upperIndex))
  }

  def countFailures(stocksReturns: Seq[Array[Double]], valueAtRisk: Double): Int = {
    var failures = 0
    for (i <- stocksReturns.head.indices) {
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
    val total = stocksReturns.head.length
    val testStatistic = kupiecTestStatistic(total, failures, confidenceLevel)
    1 - new ChiSquaredDistribution(1.0).cumulativeProbability(testStatistic)
  }

  def computeTrialReturns(
      stocksReturns: Seq[Array[Double]],
      factorsReturns: Seq[Array[Double]],
      baseSeed: Long,
      numTrials: Int,
      parallelism: Int): Dataset[Double] = {
    val factorMat = factorMatrix(factorsReturns)
    val factorCov = new Covariance(factorMat).getCovarianceMatrix().getData()
    val factorMeans = factorsReturns.map(factor => factor.sum / factor.size).toArray
    val factorFeatures = factorMat.map(featurize)
    val factorWeights = computeFactorWeights(stocksReturns, factorFeatures)

    // Generate different seeds so that our simulations don't all end up with the same results
    val seeds = (baseSeed until baseSeed + parallelism)
    val seedDS = seeds.toDS().repartition(parallelism)

    // Main computation: run simulations and compute aggregate return for each
    seedDS.flatMap(trialReturns(_, numTrials / parallelism, factorWeights, factorMeans, factorCov))
  }
}
