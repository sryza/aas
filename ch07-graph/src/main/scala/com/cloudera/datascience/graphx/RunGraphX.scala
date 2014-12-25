/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.graphx

import com.cloudera.datascience.common.XmlInputFormat

import com.google.common.hash.Hashing

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.xml._

object RunGraphX extends Serializable {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("GraphX"))

    val medline_raw = loadMedline(sc, "hdfs:///user/ds/medline")
    val mxml = medline_raw.map(XML.loadString)
    val medline = mxml.map(majorTopics).cache()

    val N = medline.count()
    val topics = medline.flatMap(mesh => mesh)
    val topicCounts = topics.countByValue()
    val tcSeq = topicCounts.toSeq
    tcSeq.sortBy(_._2).reverse.take(10).foreach(println)
    val valueDist = topicCounts.groupBy(_._2).mapValues(_.size)
    valueDist.toSeq.sorted.take(10).foreach(println)

    val topicPairs = medline.flatMap(t => t.sorted.combinations(2))
    val cooccurs = topicPairs.map(p => (p, 1)).reduceByKey(_+_)
    cooccurs.cache()
    cooccurs.count()

    cooccurs.top(10)(Ordering.by[(Seq[String], Int), Int](_._2)).foreach(println) 

    val vertices = topics.map(topic => (hashId(topic), topic))
    val edges = cooccurs.map(p => {
     val (topics, cnt) = p
     val ids = topics.map(hashId).sorted
     Edge(ids(0), ids(1), cnt)
    })
    val g = Graph(vertices, edges)
    g.cache()

    val ccg = g.connectedComponents()
    val componentCounts = ccg.vertices.map(_._2).countByValue()
    val ccSeq = componentCounts.toSeq
    ccSeq.sortBy(_._2).reverse.take(10).foreach(println)

    val nameCID = g.vertices.innerJoin(ccg.vertices) {
      (id, name, cid) => (name, cid)
    }
    val c1 = nameCID.filter(x => x._2._2 == -6468702387578666337L)
    c1.collect().foreach(x => println(x._2._1))

    val hiv = topics.filter(_.contains("HIV")).countByValue
    hiv.foreach(println)

    val degrees = g.degrees.cache()
    degrees.map(_._2).stats()
    var namesAndDegrees = degrees.innerJoin(g.vertices) {
      (id, d, name) => (name, d)
    }
    val ord = Ordering.by[(String, Int), Int](_._2)
    namesAndDegrees.map(_._2).top(10)(ord).foreach(println)

    val vc = topics.map(x => (hashId(x), 1)).reduceByKey(_+_)
    val bg = Graph(vc, g.edges)
    val cg = bg.mapTriplets(trip => {
      chiSq(trip.attr, trip.srcAttr, trip.dstAttr, N)
    })
    cg.edges.map(x => x.attr).stats()

    val sg = cg.subgraph(trip => trip.attr > 19.5)

    val sgc = sg.connectedComponents()
    val scc = sgc.vertices.map(_._2).countByValue
    scc.toSeq.sortBy(_._2).reverse.take(10).foreach(println)

    val sd = sg.degrees.cache()
    sd.map(_._2).stats()

    val snd = sd.innerJoin(g.vertices) {
      (id, d, name) => (name, d)
    }
    snd.map(_._2).top(10)(ord).foreach(println)

    val avgCC = avgClusteringCoef(sg)

    val paths = samplePathLengths(sg)
    paths.map(_._3).filter(_ > 0).stats()

    val hist = paths.map(_._3).countByValue()
    hist.toSeq.sorted.foreach(println)
  }

  def avgClusteringCoef(g: Graph[_, _]): Double = {
    val tri = g.triangleCount()
    val den = g.degrees.mapValues(d => d * (d - 1) / 2.0)
    val cc = tri.vertices.innerJoin(den) { (id, tc, den) =>
      if (den == 0) { 0 } else { tc / den }
    }
    cc.map(_._2).sum() / g.vertices.count()
  }

  def samplePathLengths[V, E](sg: Graph[V, E], fraction: Double = 0.02)
    : RDD[(VertexId, VertexId, Int)] = {
    val replacement = false
    val sample = sg.vertices.map(_._1).sample(
      replacement, fraction, 1729L)
    val ids = sample.collect().toSet

    val mg = sg.mapVertices((id, v) => {
      if (ids.contains(id)) {
        Map(id -> 0)
      } else {
        Map[VertexId, Int]()
      }
    })

    val start = Map[VertexId, Int]()
    val res = mg.ops.pregel(start)(update, iterate, addMaps)
    res.vertices.flatMap { case (id, m) =>
      m.map { case (k, v) =>
        if (id < k) (id, k, v) else (k, id, v)
      }
    }.distinct().cache()
  }

  def addMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int] = {
    (m1.keySet ++ m2.keySet).map {
      k => k -> math.min(
        m1.getOrElse(k, Int.MaxValue),
        m2.getOrElse(k, Int.MaxValue))
    }.toMap
  }

  def update(id: VertexId, state: Map[VertexId, Int], msg: Map[VertexId, Int])
    : Map[VertexId, Int] = {
    addMaps(state, msg)
  }

  def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId)
    : Iterator[(VertexId, Map[VertexId, Int])] = {
    val aplus = a.map { case (v, d) => v -> (d + 1) }
    if (b != addMaps(aplus, b)) {
      Iterator((bid, aplus))
    } else {
      Iterator.empty
    }
  }

  def iterate(e: EdgeTriplet[Map[VertexId, Int], _]): Iterator[(VertexId, Map[VertexId, Int])] = {
    checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++
    checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
  }

  def loadMedline(sc: SparkContext, path: String): RDD[String] = {
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<MedlineCitation ")
    conf.set(XmlInputFormat.END_TAG_KEY, "</MedlineCitation>")
    val in = sc.newAPIHadoopFile(path, classOf[XmlInputFormat],
        classOf[LongWritable], classOf[Text], conf)
    in.map(line => line._2.toString)
  }

  def majorTopics(elem: Elem): Seq[String] = {
    val dn = elem \\ "DescriptorName"
    val mt = dn.filter(n => (n \ "@MajorTopicYN").text == "Y")
    mt.map(n => n.text)
  }

  def hashId(str: String): Long = {
    Hashing.md5().hashString(str).asLong()
  }

  def chiSq(a: Int, S: Int, A: Int, N: Long): Double = {
    val F = N - S
    val B = N - A
    val b = A - a
    val c = S - a
    val d = N - b - c - a
    val inner = (a * d - b * c) - N / 2.0
    N * math.pow(inner, 2) / (A * B * S * F)
  }
}
