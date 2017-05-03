/*
 * Copyright 2015 and onwards Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.lsa

import edu.umd.cloud9.collection.XMLInputFormat
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class AssembleDocumentTermMatrix(private val spark: SparkSession) extends Serializable {
  import spark.implicits._

  /**
   * Returns a (title, content) pair.
   */
  def wikiXmlToPlainText(pageXml: String): Option[(String, String)] = {
    val page = new EnglishWikipediaPage()

    // Wikipedia has updated their dumps slightly since Cloud9 was written, so this hacky replacement is sometimes
    // required to get parsing to work.
    val hackedPageXml = pageXml.replaceFirst(
      "<text xml:space=\"preserve\" bytes=\"\\d+\">", "<text xml:space=\"preserve\">")

    WikipediaPage.readPage(page, hackedPageXml)
    if (page.isEmpty || !page.isArticle || page.isRedirect || page.isDisambiguation ||
        page.getTitle.contains("(disambiguation)")) {
      None
    } else {
      Some((page.getTitle, page.getContent))
    }
  }

  def parseWikipediaDump(path: String): Dataset[(String, String)] = {
    val conf = new Configuration()
    conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
    conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
    val kvs = spark.sparkContext.newAPIHadoopFile(path, classOf[XMLInputFormat], classOf[LongWritable],
      classOf[Text], conf)
    val rawXmls = kvs.map(_._2.toString).toDS()

    rawXmls.filter(_ != null).flatMap(wikiXmlToPlainText)
  }

  /**
   * Create a StanfordCoreNLP pipeline object to lemmatize documents.
   */
  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }

  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP)
    : Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences.asScala;
         token <- sentence.get(classOf[TokensAnnotation]).asScala) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }

  def contentsToTerms(docs: Dataset[(String, String)], stopWordsFile: String): Dataset[(String, Seq[String])] = {
    val stopWords = scala.io.Source.fromFile(stopWordsFile).getLines().toSet
    val bStopWords = spark.sparkContext.broadcast(stopWords)

    docs.mapPartitions { iter =>
      val pipeline = createNLPPipeline()
      iter.map { case (title, contents) => (title, plainTextToLemmas(contents, bStopWords.value, pipeline)) }
    }
  }

  def loadStopWords(path: String): Set[String] = {
    scala.io.Source.fromFile(path).getLines().toSet
  }

  /**
   * Returns a document-term matrix where each element is the TF-IDF of the row's document and
   * the column's term.
   *
   * @param docTexts a DF with two columns: title and text
   */
  def documentTermMatrix(docTexts: Dataset[(String, String)], stopWordsFile: String, numTerms: Int)
    : (DataFrame, Array[String], Map[Long, String], Array[Double]) = {
    val terms = contentsToTerms(docTexts, stopWordsFile)

    val termsDF = terms.toDF("title", "terms")
    val filtered = termsDF.where(size($"terms") > 1)

    val countVectorizer = new CountVectorizer()
      .setInputCol("terms").setOutputCol("termFreqs").setVocabSize(numTerms)
    val vocabModel = countVectorizer.fit(filtered)
    val docTermFreqs = vocabModel.transform(filtered)

    val termIds = vocabModel.vocabulary

    docTermFreqs.cache()

    val docIds = docTermFreqs.rdd.map(_.getString(0)).zipWithUniqueId().map(_.swap).collect().toMap

    val idf = new IDF().setInputCol("termFreqs").setOutputCol("tfidfVec")
    val idfModel = idf.fit(docTermFreqs)
    val docTermMatrix = idfModel.transform(docTermFreqs).select("title", "tfidfVec")

    (docTermMatrix, termIds, docIds, idfModel.idf.toArray)
  }
}
