/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.genomics

object RunTFPrediction {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("TF Prediction"))

    // Convert phyloP data to Parquet for better performance; run once
    sc.adamBEDFeatureLoad("book/phylop_text").adamSave("book/phylop")

    // Load the human genome reference sequence
    val hg19Data = sc.broadcast(
      new TwoBitFile(
        new LocalFileByteAccess(
          new File("book/hg19.2bit"))))

    val phylopRDD = (sc.adamLoad[Feature, Nothing]("book/phylop")
      // clean up a few irregularities in the phylop data
      .filter(f => f.getStart <= f.getEnd))

    val tssRDD = (sc.adamGTFFeatureLoad("book/gencode.v18.annotation.gtf")
      .filter(_.getFeatureType.toString == "transcript")
      .map(f => (f.getContig.getContigName.toString, f.getStart)))

    val tssData = sc.broadcast(tssRDD
      // group by contig name
      .groupBy(_._1)
      // create Vector of TSS sites for each chromosome
      .map(p => (p._1, p._2.map(_._2.toLong).toVector))
      // collect into local in-memory structure for broadcasting
      .collect().toMap

    // CTCF PWM from http://dx.doi.org/10.1016/j.cell.2012.12.009
    val pwmData = sc.broadcast(Vector(
      Vector('A' -> 0.45526,'C' -> 0.04591,'G' -> 0.14554,'T' -> 0.35328).toMap,
      Vector('A' -> 0.17371,'C' -> 0.02484,'G' -> 0.75917,'T' -> 0.04228).toMap,
      Vector('A' -> 0.00012,'C' -> 0.94066,'G' -> 0.00012,'T' -> 0.05911).toMap,
      Vector('A' -> 0.00514,'C' -> 0.00008,'G' -> 0.98789,'T' -> 0.00689).toMap,
      Vector('A' -> 0.06239,'C' -> 0.93217,'G' -> 0.00087,'T' -> 0.00457).toMap,
      Vector('A' -> 0.00463,'C' -> 0.99518,'G' -> 0.00010,'T' -> 0.00010).toMap,
      Vector('A' -> 0.50749,'C' -> 0.45329,'G' -> 0.01812,'T' -> 0.02109).toMap,
      Vector('A' -> 0.00795,'C' -> 0.64065,'G' -> 0.00009,'T' -> 0.35131).toMap,
      Vector('A' -> 0.00011,'C' -> 0.99955,'G' -> 0.00023,'T' -> 0.00011).toMap,
      Vector('A' -> 0.00266,'C' -> 0.00351,'G' -> 0.00168,'T' -> 0.99215).toMap,
      Vector('A' -> 0.76351,'C' -> 0.02103,'G' -> 0.11749,'T' -> 0.09798).toMap,
      Vector('A' -> 0.00740,'C' -> 0.13144,'G' -> 0.79898,'T' -> 0.06219).toMap,
      Vector('A' -> 0.01384,'C' -> 0.38794,'G' -> 0.00008,'T' -> 0.59815).toMap,
      Vector('A' -> 0.00035,'C' -> 0.00012,'G' -> 0.98535,'T' -> 0.01419).toMap,
      Vector('A' -> 0.03987,'C' -> 0.01127,'G' -> 0.73119,'T' -> 0.21767).toMap,
      Vector('A' -> 0.15198,'C' -> 0.28197,'G' -> 0.00822,'T' -> 0.55783).toMap,
      Vector('A' -> 0.36438,'C' -> 0.31051,'G' -> 0.21245,'T' -> 0.11267).toMap))


    // Define some utility functions

    // fn for finding closest transcription start site
    // naive...make this better
    def distanceToClosest(loci: Vector[Long], query: Long): Long = {
      loci.map(x => abs(x - query)).reduce(min(_, _))
    }

    // compute a motif score based on the TF PWM
    def scorePWM(ref: String): Double = {
      val score1 = ref.sliding(pwmData.value.length).map(s => {
        s.zipWithIndex.map(p => pwmData.value(p._2)(p._1)).reduce(_ * _)
      }).reduce(max(_, _))
      val rc = SequenceUtils.reverseComplement(ref)
      val score2 = rc.sliding(pwmData.value.length).map(s => {
        s.zipWithIndex.map(p => pwmData.value(p._2)(p._1)).reduce(_ * _)
      }).reduce(max(_, _))
      max(score1, score2)
    }

    // functions for labeling the DNase peaks as binding sites or not; compute
    // overlaps between an interval and a set of intervals
    // naive impl - this only works because we know the ChIP-seq peaks are non-
    // overlapping (how do we verify this? exercise for the reader)
    def isOverlapping(i1: (Long, Long), i2: (Long, Long)) = (i1._2 > i2._1) && (i1._1 < i2._2)

    def isOverlappingLoci(loci: Vector[(Long, Long)], testInterval: (Long, Long)): Boolean = {
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
      val dnaseRDD = sc.adamNarrowPeakFeatureLoad(
        "book/dnase/%s.DNase.narrowPeak".format(cellLine))
      val chipseqRDD = sc.adamNarrowPeakFeatureLoad(
        "book/chip-seq/%s.ChIP-seq.CTCF.narrowPeak".format(cellLine))

      // generate the fn for labeling the data points
      val bindingData = sc.broadcast(chipseqRDD
        // group peaks by chromosome
        .groupBy(_.getContig.getContigName.toString) // RDD[(String, Iterable[Feature])]
        // for each chr, for each ChIP-seq peak, extract start and end
        .map(p => (p._1, p._2.map(f => (f.getStart: Long, f.getEnd: Long)))) // RDD[(String, Iterable[(Long, Long)])]
        // for each chr, sort the peaks (we know they're not overlapping)
        .map(p => (p._1, p._2.toVector.sortBy(x => x._1))) // RDD[(String, Vector[(Long, Long)])]
        // collect them back into a local in-memory data structure for broadcasting
        .collect().toMap)

      def generateLabel(f: Feature) = {
        val contig = f.getContig.getContigName
        if (!bindingData.value.contains(contig)) {
          false
        } else {
          val testInterval = (f.getStart: Long, f.getEnd: Long)
          isOverlappingLoci(bindingData.value(contig), testInterval)
        }
      }

      // join the DNase peak data with conservation data to generate those features
      val dnaseWithPhylopRDD = (RegionJoin.partitionAndJoin(sc, dnaseRDD, phylopRDD)
        // group the conservation values by DNase peak
        .groupBy(x => x._1.getFeatureId.toString)
        // compute conservation stats on each peak
        .map(x => {
          val y = x._2.toSeq
          val peak = y(0)._1
          val values = y.map(_._2.getValue)
          // compute phylop features
          val avg = values.reduce(_ + _) / values.length
          val m = values.reduce(max(_, _))
          val M = values.reduce(min(_, _))
        (peak.getFeatureId, peak, avg, m, M)
      }))

      dnaseWithPhylopRDD.map(tup => {
        val peak = tup._2
        val featureId = peak.getFeatureId
        val contig = peak.getContig.getContigName
        val start = peak.getStart
        val end = peak.getEnd
        val score = scorePWM(hg19Data.value.extract(ReferenceRegion(peak)))
        val avg = tup._3
        val m = tup._4
        val M = tup._5
        val closest_tss = min(
          distanceToClosest(tssData.value(contig), peak.getStart),
          distanceToClosest(tssData.value(contig), peak.getEnd))
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
