val prefix = "/home/sandy/datascience/book/risk/"
val prefix = ""

import com.cloudera.datascience.risk._
import com.cloudera.datascience.risk.ComputeFactorWeights._
import com.cloudera.datascience.risk.MonteCarloReturns._
import java.io.File
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import com.github.nscala_time.time.Imports._
import breeze.plot._

val fiveYears = 260 * 5+10
val start = new DateTime(2009, 10, 23, 0, 0)
val end = new DateTime(2014, 10, 23, 0, 0)

val stocks1 = readHistories(new File(prefix + "data/stocks/")).filter(_.size >= fiveYears)
val stocks = stocks1.map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))

val factorsPrefix = prefix + "data/factors/"
val factors1 = Array("crudeoil.tsv", "us30yeartreasurybonds.tsv").map(x => new File(factorsPrefix + x)).map(readInvestingDotComHistory)
val factors2 = Array("SNP.csv", "NDX.csv").map(x => new File(factorsPrefix + x)).map(readYahooHistory)
val factors = (factors1 ++ factors2).map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))

val stocksReturns = stocks.map(twoWeekReturns)
val factorsReturns = factors.map(twoWeekReturns)
//val squaredFactorsReturns = factorsReturns.map(_.map(x => x * x))
//val finalFactorsReturns = factorsReturns ++ squaredFactorsReturns

val factorMat = factorMatrix(factorsReturns)

val models = stocksReturns.map(linearModel(_, factorMat))
val rSquareds = models.map(_.calculateRSquared())
//val factorWeights = models.map(_.estimateRegressionParameters()).toArray
val factorWeights = Array.ofDim[Double](stocksReturns.length, factors.length+1)
for (s <- 0 until stocksReturns.length) {
  factorWeights(s) = models(s).estimateRegressionParameters()
}
//for (s <- 0 until stocksReturns.length) {
//  val params = models(s).estimateRegressionParameters()
//  for (f <- 0 until params.length) {
//    factorWeights(s)(f) = params(f)
//  }
//}

val factorCor = new PearsonsCorrelation(factorMat).getCorrelationMatrix().getData()
println(factorCor.map(_.mkString("\t")).mkString("\n"))

val factorCov = new Covariance(factorMat).getCovarianceMatrix().getData()
println(factorCov.map(_.mkString("\t")).mkString("\n"))

val factorMeans = factorsReturns.map(factor => factor.sum / factor.size)

// Send all instruments to every node
val broadcastInstruments = sc.broadcast(factorWeights)

/*val broadcastInstruments = {
  val arr = factorWeights
  sc.broadcast(arr)
}*/

// Simulation parameters
val parallelism = 1
val baseSeed = 1496L

// Generate different seeds so that our simulations don't all end up with the same results
val seeds = (baseSeed until baseSeed + parallelism)
val seedRdd = sc.parallelize(seeds, parallelism)

val numTrials = 1
// Main computation: run simulations and compute aggregate return for each
val trialReturns = seedRdd.flatMap(trialValues(_, numTrials / parallelism, broadcastInstruments.value, factorMeans, factorCov))
/*
{
  val arr = factorWeights
  val means = factorMeans
  val covs = factorCov
  val func = (seed: Long) => trialValues(seed, numTrials / parallelism, arr, means, covs)
  val seedRdd2 = seedRdd
  seedRdd2.flatMap(func)
}
*/

// Cache the results so that we don't recompute for both of the summarizations below
//trialReturns.cache()

// Calculate VaR
val topLosses = trialReturns.takeOrdered(math.max(numTrials / 20, 1))
val varFivePercent = topLosses.last

// Plot distribution
val domain = Range.Double(20.0, 60.0, .2).toArray
val densities = KernelDensity.estimate(trialsRdd, 0.25, domain)

