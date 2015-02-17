/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.genomics

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.utils.parquet.io.LocalFileByteAccess
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.BroadcastRegionJoin
import org.bdgenomics.adam.util.{TwoBitFile, SequenceUtils}
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.adam.rich.ReferenceMappingContext._

import scala.annotation.tailrec

object RunTFPrediction {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("TF Prediction"))

    // Load the human genome reference sequence
    val bHg19Data = sc.broadcast(
      new TwoBitFile(
        new LocalFileByteAccess(
          new File("/user/ds/genomics/hg19.2bit"))))

    val phylopRDD = sc.adamLoad[Feature, Nothing]("/user/ds/genomics/phylop")
      // clean up a few irregularities in the phylop data
      .filter(f => f.getStart <= f.getEnd)

    val tssRDD = sc.loadFeatures("/user/ds/genomics/gencode.v18.annotation.gtf")
      .filter(_.getFeatureType == "transcript")
      .map(f => (f.getContig.getContigName, f.getStart))

    val bTssData = sc.broadcast(tssRDD
      // group by contig name
      .groupBy(_._1)
      // create Vector of TSS sites for each chromosome
      .map(p => (p._1, p._2.map(_._2.toLong).toVector))
      // collect into local in-memory structure for broadcasting
      .collect().toMap)

    // CTCF PWM from http://dx.doi.org/10.1016/j.cell.2012.12.009
    // generated with genomics/src/main/python/pwm.py
    val bPwmData = sc.broadcast(Vector(
      Map('A'->0.4553,'C'->0.0459,'G'->0.1455,'T'->0.3533),
      Map('A'->0.1737,'C'->0.0248,'G'->0.7592,'T'->0.0423),
      Map('A'->0.0001,'C'->0.9407,'G'->0.0001,'T'->0.0591),
      Map('A'->0.0051,'C'->0.0001,'G'->0.9879,'T'->0.0069),
      Map('A'->0.0624,'C'->0.9322,'G'->0.0009,'T'->0.0046),
      Map('A'->0.0046,'C'->0.9952,'G'->0.0001,'T'->0.0001),
      Map('A'->0.5075,'C'->0.4533,'G'->0.0181,'T'->0.0211),
      Map('A'->0.0079,'C'->0.6407,'G'->0.0001,'T'->0.3513),
      Map('A'->0.0001,'C'->0.9995,'G'->0.0002,'T'->0.0001),
      Map('A'->0.0027,'C'->0.0035,'G'->0.0017,'T'->0.9921),
      Map('A'->0.7635,'C'->0.0210,'G'->0.1175,'T'->0.0980),
      Map('A'->0.0074,'C'->0.1314,'G'->0.7990,'T'->0.0622),
      Map('A'->0.0138,'C'->0.3879,'G'->0.0001,'T'->0.5981),
      Map('A'->0.0003,'C'->0.0001,'G'->0.9853,'T'->0.0142),
      Map('A'->0.0399,'C'->0.0113,'G'->0.7312,'T'->0.2177),
      Map('A'->0.1520,'C'->0.2820,'G'->0.0082,'T'->0.5578),
      Map('A'->0.3644,'C'->0.3105,'G'->0.2125,'T'->0.1127)))


    // Define some utility functions

    // fn for finding closest transcription start site
    // naive...make this better
    def distanceToClosest(loci: Vector[Long], query: Long): Long = {
      loci.map(x => math.abs(x - query)).min
    }

    // compute a motif score based on the TF PWM
    def scorePWM(ref: String): Double = {
      val score1 = ref.sliding(bPwmData.value.length).map(s => {
        s.zipWithIndex.map(p => bPwmData.value(p._2)(p._1)).product
      }).max
      val rc = SequenceUtils.reverseComplement(ref)
      val score2 = rc.sliding(bPwmData.value.length).map(s => {
        s.zipWithIndex.map(p => bPwmData.value(p._2)(p._1)).product
      }).max
      math.max(score1, score2)
    }

    // functions for labeling the DNase peaks as binding sites or not; compute
    // overlaps between an interval and a set of intervals
    // naive impl - this only works because we know the ChIP-seq peaks are non-
    // overlapping (how do we verify this? exercise for the reader)
    def isOverlapping(i1: (Long, Long), i2: (Long, Long)) = (i1._2 > i2._1) && (i1._1 < i2._2)

    def isOverlappingLoci(loci: Vector[(Long, Long)], testInterval: (Long, Long)): Boolean = {
      @tailrec
      def search(m: Int, M: Int): Boolean = {
        val mid = m + (M - m) / 2
        if (M <= m) {
          false
        } else if (isOverlapping(loci(mid), testInterval)) {
          true
        } else if (testInterval._2 <= loci(mid)._1) {
          search(m, mid)
        } else {
          search(mid + 1, M)
        }
      }
      search(0, loci.length)
    }

    val cellLines = Vector("GM12878", "K562", "BJ", "HEK293", "H54", "HepG2")

    val dataByCellLine = cellLines.map(cellLine => {
      val dnaseRDD = sc.loadFeatures(
        s"/user/ds/genomics/dnase/$cellLine.DNase.narrowPeak")
      val chipseqRDD = sc.loadFeatures(
        s"/user/ds/genomics/chip-seq/$cellLine.ChIP-seq.CTCF.narrowPeak")

      // generate the fn for labeling the data points
      val bBindingData = sc.broadcast(chipseqRDD
        // group peaks by chromosome
        .groupBy(_.getContig.getContigName) // RDD[(String, Iterable[Feature])]
        // for each chr, for each ChIP-seq peak, extract start and end
        .map(p => (p._1, p._2.map(f => (f.getStart: Long, f.getEnd: Long)))) // RDD[(String, Iterable[(Long, Long)])]
        // for each chr, sort the peaks (we know they're not overlapping)
        .map(p => (p._1, p._2.toVector.sortBy(x => x._1))) // RDD[(String, Vector[(Long, Long)])]
        // collect them back into a local in-memory data structure for broadcasting
        .collect().toMap)

      def generateLabel(f: Feature) = {
        val contig = f.getContig.getContigName
        if (!bBindingData.value.contains(contig)) {
          false
        } else {
          val testInterval = (f.getStart: Long, f.getEnd: Long)
          isOverlappingLoci(bBindingData.value(contig), testInterval)
        }
      }

      // join the DNase peak data with conservation data to generate those features
      val dnaseWithPhylopRDD = BroadcastRegionJoin.partitionAndJoin(sc, dnaseRDD, phylopRDD)
        // group the conservation values by DNase peak
        .groupBy(x => x._1.getFeatureId)
        // compute conservation stats on each peak
        .map(x => {
          val y = x._2.toSeq
          val peak = y(0)._1
          val values = y.map(_._2.getValue)
          // compute phylop features
          val avg = values.reduce(_ + _) / values.length
          val m = values.max
          val M = values.min
          (peak.getFeatureId, peak, avg, m, M)
        })

      dnaseWithPhylopRDD.map(tup => {
        val peak = tup._2
        val featureId = peak.getFeatureId
        val contig = peak.getContig.getContigName
        val start = peak.getStart
        val end = peak.getEnd
        val score = scorePWM(bHg19Data.value.extract(ReferenceRegion(peak)))
        val avg = tup._3
        val m = tup._4
        val M = tup._5
        val closest_tss = math.min(
          distanceToClosest(bTssData.value(contig), peak.getStart),
          distanceToClosest(bTssData.value(contig), peak.getEnd))
        val tf = "CTCF"
        val line = cellLine
        val bound = generateLabel(peak)
        (featureId, contig, start, end, score, avg, m, M, closest_tss, tf, line, bound)
      })
    })

    // union the prepared data together
    val preTrainingData = dataByCellLine.reduce(_ ++ _)
    preTrainingData.cache()
    preTrainingData.count() // 801263
    preTrainingData.filter(_._12 == true).count() // 220285

    // carry on into classification

  }
}
