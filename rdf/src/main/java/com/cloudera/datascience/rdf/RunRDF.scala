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
    decisionTree(rawData)
  }

  def decisionTree(rawData: RDD[String]): Unit = {
    val data = rawData.map { line =>
      val values = line.split(',').map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }

    val trainAndCVAndTestData = data.randomSplit(Array(0.8, 0.1, 0.1))
    val trainData = trainAndCVAndTestData(0).cache()
    val cvData = trainAndCVAndTestData(1).cache()
    val testData = trainAndCVAndTestData(2).cache()

    val model = DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](), "gini", 4, 100)

    val predictionsAndLabels = cvData.map(l => (model.predict(l.features), l.label))
    val metrics = new MulticlassMetrics(predictionsAndLabels)

    println(metrics.confusionMatrix)
  }

}