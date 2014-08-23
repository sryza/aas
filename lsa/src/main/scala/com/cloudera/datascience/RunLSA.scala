/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.datascience

import com.cloudera.datascience.ParseWikipedia._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Matrix, SingularValueDecomposition}
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object RunLSA {
  def main(args: Array[String]) {
    val k = if (args.length > 0) args(0).toInt else 100
    val numTerms = if (args.length > 1) args(1).toInt else 50000
    val sampleSize = if (args.length > 2) args(2).toDouble else 0.1

    val conf = new SparkConf().setAppName("Wiki LSA")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val (termDocMatrix, termIds) = preprocessing(sampleSize, numTerms, sc)
    termDocMatrix.cache()
    //println("termDocMatrix num rows: " + termDocMatrix.count())
    val mat = new RowMatrix(termDocMatrix)
    val svd = mat.computeSVD(k)

    println("Singular values: " + svd.s)
    val topTermsTopConcepts = topTermsInTopConcepts(svd, 100, 15, termIds)
    for (concept <- topTermsTopConcepts) {
      println("Concept: " + concept.mkString(","))
      println()
    }
  }

  def preprocessing(sampleSize: Double, numTerms: Int, sc: SparkContext)
      : (RDD[Vector], Map[Int, String]) = {
    val pages = readFile("/user/srowen/DataSets/Wikipedia/20131205/", sc)
      .sample(false, sampleSize, 11L)

    val plainText = pages.filter(_ != null).map(wikiXmlToPlainText).filter(_.length > 0)

    val stopWords = sc.broadcast(loadStopWords("stopwords.txt")).value

    val lemmatized = plainText.mapPartitions(iter => {
      val pipeline = createPipeline()
      iter.map(plainTextToLemmas(_, stopWords, pipeline))
    })

    val filtered = lemmatized.filter(_.size > 1)

    termDocumentMatrix(filtered, stopWords, numTerms, sc)
  }

  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numTerms: Int, termIds: Map[Int, String]): Seq[Seq[(String, Double)]] = {
    // TODO: can we make it easier to actually look at the insides of matrices
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = v.toArray.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(_._1)
      topTerms += sorted.takeRight(numTerms).map{case (score, id) => (termIds(id), score)}
    }
    topTerms.map(_.reverse)
  }

//  def topDocsInTopConcepts

}
