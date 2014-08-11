package com.cloudera.datascience

import java.io.StringReader
import java.util.{Properties, StringTokenizer, HashMap, ArrayList, Arrays}

import javax.xml.stream.{XMLInputFactory, XMLStreamConstants}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors, Vector}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import edu.stanford.nlp.pipeline.{MorphaAnnotator, Annotation, StanfordCoreNLP}
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.util.{CoreMap, ArrayCoreMap}
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation

case class Page(title: String, contents: String)

object ParseWikipedia {

  def addTermCount(map: HashMap[String, Int], term: String, count: Int) {
    map.put(term, (map.getOrElse(term, 0) + count))
  }

  /**
   * Returns a term-document matrix where each element is the TF-IDF of the row's document and
   * the column's term.
   */
  def termDocumentMatrix(docs: RDD[Seq[String]], stopWords: Set[String], sc: SparkContext):
      RDD[Vector] = {
    val docTermFreqs = docs.map(terms => {
      val termFreqsInDoc = terms.foldLeft(new HashMap[String, Int]()) {
        (map, term) => {
          addTermCount(map, term, 1)
          map
        }
      }
      termFreqsInDoc
    })
    docTermFreqs.cache()
    val numDocs = docTermFreqs.count().toInt

    val idfs = inverseDocumentFrequencies(docTermFreqs, numDocs)

    // maps terms to their indexes in the vector
    val termIndexes = new HashMap[String, Int]()
    var index = 0
    for (term <- idfs.keySet()) {
      termIndexes += (term -> index)
      index += 1
    }

    val bIdfs = sc.broadcast(idfs).value

    val vecs = docTermFreqs.map(docTermFreqs => {
      val docTotalTerms = docTermFreqs.values().sum
      val termScores = docTermFreqs.map{
        case (term, freq) => (termIndexes(term), bIdfs(term) * docTermFreqs(term) / docTotalTerms)
      }.toSeq
      Vectors.sparse(index, termScores)
    })
    vecs
  }

  def inverseDocumentFrequencies(docTermFreqs: RDD[HashMap[String, Int]], numDocs: Int):
      HashMap[String, Double] = {
    val docFreqs = docTermFreqs.aggregate(new HashMap[String, Int]())(
      (dfs, tfs) => {
        tfs.keySet().foreach{ term =>
          addTermCount(dfs, term, 1)
        }
        dfs
      },
      (dfs1, dfs2) => {
        for ((term, count) <- dfs2) {
          addTermCount(dfs1, term, count)
        }
        dfs1
      }
    )
    val idfs = new HashMap[String, Double]()
    for ((term, count) <- docFreqs) {
      idfs.put(term, math.log(numDocs / count))
    }
    idfs
  }

  def tfidf(term: String, docTermFreqs: HashMap[String, Int], totalTerms: Int,
      docFreqs: HashMap[String, Int], totalDocs: Int): Double = {
    // TODO: can calculate this in the driver
    val idf = math.log(totalDocs / docFreqs(term))
    val tf = docTermFreqs(term) / totalTerms
    tf * idf
  }

  def readFile(path: String, sc: SparkContext): RDD[Page] = {
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</page>")
    val rawXmls = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable],
      classOf[Text], conf)
    rawXmls.map(p => parsePage(p._2.toString))
  }

  def parsePage(pageXml: String): Page = {
    val factory = XMLInputFactory.newInstance()
    val streamParser = factory.createXMLStreamReader(new StringReader(pageXml))
    val titleBuilder = new StringBuilder()
    val textBuilder = new StringBuilder()
    var inTitle = false
    var inText = false
    while (streamParser.hasNext()) {
      val event = streamParser.next()
      event match {
        case XMLStreamConstants.START_ELEMENT => {
          if (inTitle) {
            titleBuilder.append("<" + streamParser.getLocalName() + ">")
          } else if (inText) {
            textBuilder.append("<" + streamParser.getLocalName() + ">")
          }
          if (streamParser.getLocalName().equalsIgnoreCase("title")) {
            inTitle = true
          } else if (streamParser.getLocalName().equalsIgnoreCase("text")) {
            inText = true
          }
        }
        case XMLStreamConstants.END_ELEMENT => {
          if (streamParser.getLocalName().equalsIgnoreCase("title")) {
            inTitle = false
          } else if (streamParser.getLocalName().equalsIgnoreCase("text")) {
            inText = false
          }
          if (inTitle) {
            titleBuilder.append("</" + streamParser.getLocalName() + ">")
          } else if (inText) {
            textBuilder.append("</" + streamParser.getLocalName() + ">")
          }
        }
        case XMLStreamConstants.CHARACTERS => {
          if (inTitle) {
            titleBuilder.append(streamParser.getText())
          } else if (inText) {
            textBuilder.append(streamParser.getText())
          }
        }
        case _ =>
      }
    }
    new Page(titleBuilder.toString(), textBuilder.toString())
  }

  def lemmatize(words: Seq[String]): Seq[String] = {
    val coreLabels = new ArrayList[CoreLabel]()
    words.foreach{ word =>
      val label = new CoreLabel()
      label.setWord(word)
      coreLabels += label
    }

    val sentence = new ArrayCoreMap()
    sentence.set(classOf[CoreAnnotations.TokensAnnotation], coreLabels)
    val sentences = new ArrayList[CoreMap](1)
    sentences.add(sentence)
    val doc = new Annotation(sentences)

    val annotator = new MorphaAnnotator()
    annotator.annotate(doc)

    coreLabels.map(_.get(classOf[LemmaAnnotation]))
  }

  def stripFormattingAndFindTerms(str: String, stopWords: Set[String]): Seq[String] = {
    val tokenizer = new StringTokenizer(str)
    val terms = new ArrayBuffer[String]()
    while (tokenizer.hasMoreTokens) {
      val token = tokenizer.nextToken()
      if (!token.startsWith("<") && !token.endsWith(">") && !token.startsWith("{")
          && !token.endsWith("}")) {
        var trimmed = trimFormatting(token)
        if (trimmed.endsWith("'s")) {
          trimmed = trimmed.substring(0, trimmed.length-2)
        }

        if (trimmed.length > 0 && !stopWords.contains(trimmed) && isOnlyLetters(trimmed)) {
          terms += trimmed.toLowerCase
        }
      }
    }
    terms
  }

  def trimFormatting(str: String): String = {
    var start = 0
    while (start < str.length() && !Character.isLetter(str.charAt(start))) {
      start += 1
    }
    var end = str.length - 1
    while (end >= 0 && !Character.isLetter(str.charAt(end))) {
      end -= 1
    }

    if (start >= end+1) "" else str.substring(start, end+1)
  }

  def isOnlyLetters(str: String): Boolean = {
    var i = 0
    while (i < str.length) {
      if (!Character.isLetter(str.charAt(i))) {
        return false
      }
      i += 1
    }
    true
  }
}

