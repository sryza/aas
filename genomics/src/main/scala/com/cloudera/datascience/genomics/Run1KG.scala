/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.genomics

import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Genotype

object Run1KG {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("TF Prediction"))

    val genotypesRDD = sc.adamLoad[Genotype, Nothing]("book/1kg/parquet")

  }
}
