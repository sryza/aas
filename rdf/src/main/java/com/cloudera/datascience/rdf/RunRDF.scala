/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.rdf

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RunRDF {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("RDF"))
    val rawData = sc.textFile("/user/ds/covtype.data")

    val data = rawData.map { line =>
      val values = line.split(',').map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }

    // Split into 80% train, 10% cross validation, 10% test
    val trainAndCVAndTestData = data.randomSplit(Array(0.8, 0.1, 0.1))
    val trainData = trainAndCVAndTestData(0).cache()
    val cvData = trainAndCVAndTestData(1).cache()
    val testData = trainAndCVAndTestData(2).cache()

    simpleDecisionTree(trainData, cvData)
    randomClassifier(trainData, cvData)
  }

  def simpleDecisionTree(trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]): Unit = {
    // Build a simple default DecisionTreeModel
    val model = DecisionTree.trainClassifier(trainData, 7, Map[Int, Int](), "gini", 4, 100)

    val predictionsAndLabels = cvData.map(l => (model.predict(l.features), l.label))
    val metrics = new MulticlassMetrics(predictionsAndLabels)

    println(metrics.confusionMatrix)
    println(metrics.precision)

    (0 until 7).map(
      category => (metrics.precision(category), metrics.recall(category))
    ).foreach(println)
  }

  def randomClassifier(trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]): Unit = {
    val trainPriorProbabilities = classProbabilities(trainData)
    val cvPriorProbabilities = classProbabilities(cvData)
    val precision = trainPriorProbabilities.zip(cvPriorProbabilities).map {
      case (trainProb, cvProb) => trainProb * cvProb
    }.sum
    println(precision)
  }

  def classProbabilities(data: RDD[LabeledPoint]): Array[Double] = {
    // Count (category,count) in data
    val countsByCategory = data.map(_.label).countByValue()
    // order counts by category and extract counts
    val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)
    counts.map(_.toDouble / counts.sum)
  }

}