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
import org.apache.spark.mllib.tree.model.DecisionTreeModel
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
    evaluate(trainData, cvData, testData)

    trainData.unpersist()
    cvData.unpersist()
    testData.unpersist()
  }

  def simpleDecisionTree(trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]): Unit = {
    // Build a simple default DecisionTreeModel
    val model = DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](), "gini", 4, 100)

    val metrics = getMetrics(model, cvData)

    println(metrics.confusionMatrix)
    println(metrics.precision)

    (0 until 7).map(
      category => (metrics.precision(category), metrics.recall(category))
    ).foreach(println)
  }

  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    val predictionsAndLabels = data.map(example =>
      (model.predict(example.features), example.label)
    )
    new MulticlassMetrics(predictionsAndLabels)
  }

  def randomClassifier(trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]): Unit = {
    val trainPriorProbabilities = classProbabilities(trainData)
    val cvPriorProbabilities = classProbabilities(cvData)
    val accuracy = trainPriorProbabilities.zip(cvPriorProbabilities).map {
      case (trainProb, cvProb) => trainProb * cvProb
    }.sum
    println(accuracy)
  }

  def classProbabilities(data: RDD[LabeledPoint]): Array[Double] = {
    // Count (category,count) in data
    val countsByCategory = data.map(_.label).countByValue()
    // order counts by category and extract counts
    val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)
    counts.map(_.toDouble / counts.sum)
  }

  def evaluate(trainData: RDD[LabeledPoint],
               cvData: RDD[LabeledPoint],
               testData: RDD[LabeledPoint]): Unit = {

    val evaluations =
      for (impurity <- Array("gini", "entropy");
           depth    <- Array(1, 20);
           bins     <- Array(10, 300))
        yield {
          val model = DecisionTree.trainClassifier(
            trainData, 7, Map[Int,Int](), impurity, depth, bins)
          val accuracy = getMetrics(model, cvData).precision
          ((impurity, depth, bins), accuracy)
        }

    evaluations.sortBy(_._2).reverse.foreach(println)

    val model = DecisionTree.trainClassifier(trainData.union(cvData), 7, Map[Int,Int](), "entropy", 20, 300)
    println(getMetrics(model, testData).precision)
    println(getMetrics(model, trainData.union(cvData)).precision)
  }

  def undoOneHot(rawData: RDD[String]): Unit = {
    val data = rawData.map { line =>
      val values = line.split(',').map(_.toDouble)
      val wilderness = values.slice(10, 14).indexOf(1.0).toDouble
      val soil = values.slice(14, 54).indexOf(1.0).toDouble
      val featureVector = Vectors.dense(values.slice(0, 10) :+ wilderness :+ soil)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }

    val trainAndCVAndTestData = data.randomSplit(Array(0.8, 0.1, 0.1))
    val trainData = trainAndCVAndTestData(0).cache()
    val cvData = trainAndCVAndTestData(1).cache()
    val testData = trainAndCVAndTestData(2).cache()

    val evaluations =
      for (impurity <- Array("gini", "entropy");
           depth    <- Array(12, 18);
           bins     <- Array(40, 300))
      yield {
        val model = DecisionTree.trainClassifier(
          trainData, 7, Map(10 -> 4, 11 -> 40), impurity, depth, bins)
        val trainAccuracy = getMetrics(model, trainData).precision
        val cvAccuracy = getMetrics(model, cvData).precision
        ((impurity, depth, bins), (trainAccuracy, cvAccuracy))
      }

    evaluations.sortBy(_._2._2).reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()
    testData.unpersist()
  }

}