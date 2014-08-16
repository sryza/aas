package com.cloudera.datascience

import java.io.{FileOutputStream, File, PrintStream, StringReader}
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

import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.ling.CoreAnnotations.{TokensAnnotation, SentencesAnnotation, LemmaAnnotation}

import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage
import edu.umd.cloud9.collection.wikipedia.WikipediaPage

case class Page(title: String, contents: String)

object ParseWikipedia {

  private def addTermCount(map: HashMap[String, Int], term: String, count: Int) {
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
//    docTermFreqs.cache()
    val docFreqs = documentFrequencies(docTermFreqs)
    print("number of terms: " + docFreqs.size)
    saveDocFreqs("docfreqs.tsv", docFreqs)
    System.exit(0)
    val numDocs = docTermFreqs.count().toInt

    val idfs = inverseDocumentFrequencies(docFreqs, numDocs)

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

  def documentFrequencies(docTermFreqs: RDD[HashMap[String, Int]]): HashMap[String, Int] = {
    docTermFreqs.aggregate(new HashMap[String, Int]())(
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
  }

  def inverseDocumentFrequencies(docFreqs: HashMap[String, Int], numDocs: Int):
      HashMap[String, Double] = {
    val idfs = new HashMap[String, Double]()
    for ((term, count) <- docFreqs) {
      idfs.put(term, math.log(numDocs / count))
    }
    idfs
  }

  def readFile(path: String, sc: SparkContext): RDD[String] = {
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</page>")
    val rawXmls = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable],
      classOf[Text], conf)
    rawXmls.map(p => p._2.toString)
  }

  def wikiXmlToPlainText(pageXml: String): String = {
    val page = new EnglishWikipediaPage()
    WikipediaPage.readPage(page, pageXml)
    if (page.isEmpty()) "" else page.getContent()
  }

  def createPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP)
      : Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences) {
      for (token <- sentence.get(classOf[TokensAnnotation])) {
        val lemma = token.get(classOf[LemmaAnnotation])
        if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
          lemmas += lemma
        }
      }
    }
    lemmas
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

  def loadStopWords(path: String) = scala.io.Source.fromFile(path).getLines.toSet

  def saveDocFreqs(path: String, docFreqs: HashMap[String, Int]) {
    val ps = new PrintStream(new FileOutputStream(path))
    for ((doc, freq) <- docFreqs) {
      ps.println(s"$doc\t$freq")
    }
    ps.close()
  }
}

