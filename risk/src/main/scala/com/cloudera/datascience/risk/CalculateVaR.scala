/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.risk

import com.github.nscala_time.time.Imports._

import com.cloudera.datascience.risk.ComputeFactorWeights._
import com.cloudera.datascience.risk.MonteCarloReturns._

import java.io.File

import org.apache.commons.math3.stat.correlation.Covariance

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CalculateVaR {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("VaR"))
    val (stocks, factors) = readStocksAndFactors("./")
    val numTrials = 10000000
    val parallelism = 1000
    val baseSeed = 1001L
    val trialReturns = computeTrialReturns(stocks, factors, sc, baseSeed, numTrials, parallelism)
    trialReturns.cache()
    val topLosses = trialReturns.takeOrdered(math.max(numTrials / 20, 1))
    println("VaR 5%: " + topLosses.last)
    plotDistribution(trialReturns)
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

    val broadcastInstruments = sc.broadcast(factorWeights)

    // Generate different seeds so that our simulations don't all end up with the same results
    val seeds = (baseSeed until baseSeed + parallelism)
    val seedRdd = sc.parallelize(seeds, parallelism)

    // Main computation: run simulations and compute aggregate return for each
    seedRdd.flatMap(
      trialReturns(_, numTrials / parallelism, broadcastInstruments.value, factorMeans, factorCov))
  }

  def computeFactorWeights(
      stocksReturns: Seq[Array[Double]],
      factorMat: Array[Array[Double]]): Array[Array[Double]] = {
    val models = stocksReturns.map(linearModel(_, factorMat))
    val factorWeights = Array.ofDim[Double](stocksReturns.length, factorMat.head.length+1)
    for (s <- 0 until stocksReturns.length) {
      factorWeights(s) = models(s).estimateRegressionParameters()
    }
    factorWeights
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
      map(readInvestingDotComHistory(_))
    val factors2 = Array("SNP.csv", "NDX.csv").
      map(x => new File(factorsPrefix + x)).
      map(readYahooHistory(_))

    val factors = (factors1 ++ factors2).
      map(trimToRegion(_, start, end)).
      map(fillInHistory(_, start, end))

    val stocksReturns = stocks.map(twoWeekReturns(_))
    val factorsReturns = factors.map(twoWeekReturns(_))
    (stocksReturns, factorsReturns)
  }
}
