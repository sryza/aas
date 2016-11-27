/*
 * Copyright 2015 and onwards Sanford Ryza, Juliet Hougland, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.graph

import edu.umd.cloud9.collection.XMLInputFormat

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, Row}
import org.apache.spark.sql.functions._

import scala.xml._

object RunGraph extends Serializable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val medlineRaw = loadMedline(sc, "hdfs:///user/ds/medline")
    val mxml: RDD[Elem] = medlineRaw.map(XML.loadString)
    val medline: Dataset[Seq[String]] = mxml.map(majorTopics).toDS.cache()

    val topics = medline.flatMap(mesh => mesh).toDF("topic")
    topics.createOrReplaceTempView("topics")
    val topicDist = spark.sql("SELECT topic, COUNT(*) cnt FROM topics GROUP BY topic ORDER BY cnt DESC")
    topicDist.show()
    topicDist.createOrReplaceTempView("topic_dist")
    spark.sql("SELECT cnt, COUNT(*) dist FROM topic_dist GROUP BY cnt ORDER BY dist DESC LIMIT 10").show()

    val topicPairs = medline.flatMap(t => t.sorted.combinations(2)).toDF("pairs")
    topicPairs.createOrReplaceTempView("topic_pairs")
    val cooccurs = spark.sql("SELECT pairs, COUNT(*) cnt FROM topic_pairs GROUP BY pairs")
    cooccurs.cache()

    println("Number of unique co-occurrence pairs: " + cooccurs.count())
    spark.sql("SELECT pairs, cnt FROM topic_pairs ORDER BY cnt DESC LIMIT 10").show()

    val vertices = topics.map{ case Row(topic: String) => (hashId(topic), topic) }
    val edges = cooccurs.map{ case Row(topics: Seq[_], cnt: Long) => {
       val ids = topics.map(_.toString).map(hashId).sorted
       Edge(ids(0), ids(1), cnt)
      }
    }
    val topicGraph = Graph(vertices.rdd, edges.rdd)
    topicGraph.cache()

    val connectedComponentGraph = topicGraph.connectedComponents()
    val componentCounts = sortedConnectedComponents(connectedComponentGraph)
    componentCounts.size
    componentCounts.take(10)foreach(println)

    val nameCID = topicGraph.vertices.innerJoin(connectedComponentGraph.vertices) {
      (topicId, name, componentId) => (name, componentId)
    }
    val c1 = nameCID.filter(x => x._2._2 == componentCounts(1)._1)
    c1.collect().foreach(x => println(x._2._1))

    val hiv = spark.sql("SELECT * FROM topic_dist WHERE topic LIKE '%hiv%'")
    hiv.show()

    val degrees: VertexRDD[Int] = topicGraph.degrees.cache()
    degrees.map(_._2).stats()
    topNamesAndDegrees(degrees, topicGraph).foreach(println)

    val T = medline.count()
    val topicCountsRdd = topicDist.map{ case Row(topic: String, cnt: Long) => (hashId(topic), cnt) }.rdd
    val topicCountGraph = Graph(topicCountsRdd, topicGraph.edges)
    val chiSquaredGraph = topicCountGraph.mapTriplets(triplet => {
      chiSq(triplet.attr, triplet.srcAttr, triplet.dstAttr, T)
    })
    chiSquaredGraph.edges.map(x => x.attr).stats()

    val interesting = chiSquaredGraph.subgraph(triplet => triplet.attr > 19.5)

    val interestingComponentCounts = sortedConnectedComponents(interesting.connectedComponents())
    interestingComponentCounts.size
    interestingComponentCounts.take(10).foreach(println)

    val interestingDegrees = interesting.degrees.cache()
    interestingDegrees.map(_._2).stats()
    topNamesAndDegrees(interestingDegrees, topicGraph).foreach(println)

    val avgCC = avgClusteringCoef(interesting)

    val paths = samplePathLengths(interesting)
    paths.map(_._3).filter(_ > 0).stats()

    val hist = paths.map(_._3).countByValue()
    hist.toSeq.sorted.foreach(println)
  }

  def sortedConnectedComponents(connectedComponents: Graph[VertexId, _]): Seq[(VertexId, Long)] = {
    val componentCounts = connectedComponents.vertices.map(_._2).countByValue
    componentCounts.toSeq.sortBy(_._2).reverse
  }

  def topNamesAndDegrees(degrees: VertexRDD[Int], topicGraph: Graph[String, Long])
    : Array[(String, Int)] = {
    val namesAndDegrees = degrees.innerJoin(topicGraph.vertices) {
      (topicId, degree, name) => (name, degree)
    }
    val ord = Ordering.by[(String, Int), Int](_._2)
    namesAndDegrees.map(_._2).top(10)(ord)
  }

  def avgClusteringCoef(graph: Graph[_, _]): Double = {
    val triCountGraph = graph.triangleCount()
    val maxTrisGraph = graph.degrees.mapValues(d => d * (d - 1) / 2.0)
    val clusterCoefGraph = triCountGraph.vertices.innerJoin(maxTrisGraph) {
      (vertexId, triCount, maxTris) => if (maxTris == 0) 0 else triCount / maxTris
    }
    clusterCoefGraph.map(_._2).sum() / graph.vertices.count()
  }

  def samplePathLengths[V, E](graph: Graph[V, E], fraction: Double = 0.02)
    : RDD[(VertexId, VertexId, Int)] = {
    val replacement = false
    val sample = graph.vertices.map(v => v._1).sample(
      replacement, fraction, 1729L)
    val ids = sample.collect().toSet

    val mapGraph = graph.mapVertices((id, v) => {
      if (ids.contains(id)) {
        Map(id -> 0)
      } else {
        Map[VertexId, Int]()
      }
    })

    val start = Map[VertexId, Int]()
    val res = mapGraph.ops.pregel(start)(update, iterate, mergeMaps)
    res.vertices.flatMap { case (id, m) =>
      m.map{ case (k, v) =>
        if (id < k) {
          (id, k, v)
        } else {
          (k, id, v)
        }
      }
    }.distinct().cache()
  }

  def mergeMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int] = {
    def minThatExists(k: VertexId): Int = {
      math.min(
        m1.getOrElse(k, Int.MaxValue),
        m2.getOrElse(k, Int.MaxValue))
    }

    (m1.keySet ++ m2.keySet).map{
      k => (k, minThatExists(k))
    }.toMap
  }

  def update(id: VertexId, state: Map[VertexId, Int], msg: Map[VertexId, Int])
    : Map[VertexId, Int] = {
    mergeMaps(state, msg)
  }

  def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId)
    : Iterator[(VertexId, Map[VertexId, Int])] = {
    val aplus = a.map{ case (v, d) => v -> (d + 1) }
    if (b != mergeMaps(aplus, b)) {
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
    conf.set(XMLInputFormat.START_TAG_KEY, "<MedlineCitation ")
    conf.set(XMLInputFormat.END_TAG_KEY, "</MedlineCitation>")
    val in = sc.newAPIHadoopFile(path, classOf[XMLInputFormat],
      classOf[LongWritable], classOf[Text], conf)
    in.map(line => line._2.toString)
  }

  def majorTopics(elem: Elem): Seq[String] = {
    val dn = elem \\ "DescriptorName"
    val mt = dn.filter(n => (n \ "@MajorTopicYN").text == "Y")
    mt.map(n => n.text)
  }

  def hashId(str: String): Long = {
    // This is effectively the same implementation as in Guava's Hashing, but 'inlined'
    // to avoid a dependency on Guava just for this. It creates a long from the first 8 bytes
    // of the (16 byte) MD5 hash, with first byte as least-significant byte in the long.
    val bytes = MessageDigest.getInstance("MD5").digest(str.getBytes(StandardCharsets.UTF_8))
    (bytes(0) & 0xFFL) |
    ((bytes(1) & 0xFFL) << 8) |
    ((bytes(2) & 0xFFL) << 16) |
    ((bytes(3) & 0xFFL) << 24) |
    ((bytes(4) & 0xFFL) << 32) |
    ((bytes(5) & 0xFFL) << 40) |
    ((bytes(6) & 0xFFL) << 48) |
    ((bytes(7) & 0xFFL) << 56)
  }

  def chiSq(YY: Long, YB: Long, YA: Long, T: Long): Double = {
    val NB = T - YB
    val NA = T - YA
    val YN = YA - YY
    val NY = YB - YY
    val NN = T - NY - YN - YY
    val inner = math.abs(YY * NN - YN * NY) - T / 2.0
    T * math.pow(inner, 2) / (YA * NA * YB * NB)
  }
}
