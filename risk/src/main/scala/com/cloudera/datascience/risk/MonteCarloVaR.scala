package com.cloudera.datascience.risk

import org.apache.commons.math3.random.MersenneTwister
import org.apache.commons.math3.distribution.MultivariateNormalDistribution

object MonteCarloVaR {
  def trialValues(seed: Long, numTrials: Int, instruments: Seq[Array[Double]],
      factorMeans: Array[Double], factorCovariances: Array[Array[Double]]): Seq[Double] = {
    val rand = new MersenneTwister(seed)
    val multivariateNormal = new MultivariateNormalDistribution(rand, factorMeans,
      factorCovariances)

    val trialValues = new Array[Double](numTrials)
    for (i <- 0 until numTrials) {
      val trial = multivariateNormal.sample()
      trialValues(i) = trialValue(trial, instruments)
    }
    trialValues
  }

  /**
   * Calculate the full value of the portfolio under particular trial conditions.
   */
  def trialValue(trial: Array[Double], instruments: Seq[Array[Double]]): Double = {
    var totalValue = 0.0
    for (instrument <- instruments) {
      totalValue += instrumentTrialValue(instrument, trial)
    }
    totalValue
  }

  /**
   * Calculate the value of a particular instrument under particular trial conditions.
   */
  def instrumentTrialValue(instrument: Array[Double], trial: Array[Double]): Double = {
    var instrumentTrialValue = 0.0
    var i = 0
    while (i < trial.length) {
      instrumentTrialValue += trial(i) * instrument(i)
      i += 1
    }
    instrumentTrialValue
  }
}
