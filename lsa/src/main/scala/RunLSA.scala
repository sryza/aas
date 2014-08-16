import com.cloudera.datascience.ParseWikipedia._
import org.apache.spark.{SparkContext, SparkConf}

object RunLSA {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Wiki LSA"))
    val pages = readFile("/user/srowen/DataSets/Wikipedia/20131205/", sc).sample(false, .05, 11L)

    val plainText = pages.filter(_ != null).map(wikiXmlToPlainText).filter(_.length > 0)

    val stopWords = sc.broadcast(loadStopWords("stopwords.txt")).value

    val lemmatized = plainText.mapPartitions(iter => {
      val pipeline = createPipeline()
      iter.map(plainTextToLemmas(_, stopWords, pipeline))
    })

    val filtered = lemmatized.filter(_.size > 1)
//    val count = filtered.count()
//    println("lemmatized count: " + count)

    val termDocMatrix = termDocumentMatrix(filtered, stopWords, sc)
    println("termDocMatrix num rows: " + termDocMatrix.count())
  }
}

class RunLSA {


/*  def findPrincipalComponents(termDocMat: RDD[Vector]) = {
    val mat = new RowMatrix(termDocMat)
    mat.computePrincipalComponents(mat.numCols().toInt)
  }*/
}
