
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.ml.feature
import scala.collection.mutable.ArrayBuffer

import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import org.apache.spark.rdd.RDD

import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.spark.streaming.dstream.DStream

object bigrams {

  def main(args: Array[String]) {
    def plainTextToLemmas(text: String, stopWords: Set[String]): Seq[String] = {
      //var txtStr1 = text.replaceAll("""\W""", " ").replaceAll("\\.", " ")// split on space
      //.filter(_.nonEmpty) // remove empty string
      //.mkString(" ")
      //println("Input Stream: " + txtStr1)
      val props = new Properties()
      props.put("annotators", "tokenize, ssplit, pos, lemma")
      val pipeline = new StanfordCoreNLP(props)
      val doc = new Annotation(text)
      pipeline.annotate(doc)
      val lemmas = new ArrayBuffer[String]()
      val sentences = doc.get(classOf[SentencesAnnotation])
      for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
        val lemma = token.get(classOf[LemmaAnnotation])
        //Remove Stop Words
        if (lemma.length > 2 && !stopWords.contains(lemma)) {
          lemmas += lemma.toLowerCase
        }
      }
      println("Lemmatized Out: " + lemmas)
      lemmas
    }

    val (zkQuorum, group, topics, numThreads) = ("localhost", "localhost", "test", "1")
    val sparkConf = new SparkConf().setMaster("local[*]").setSparkHome("/usr/local/spark").setAppName("bigramsStream")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(20))
    ssc.checkpoint("checkpoint")

    val stopWords = sc.broadcast(scala.io.Source.fromFile("stopWords.txt").getLines().toSet).value

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val lemmatizedDat = lines.map(plainTextToLemmas(_, stopWords).toList)
    lemmatizedDat.foreachRDD(rdd => {
      generateNgramsSerial.bigrams(rdd)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}