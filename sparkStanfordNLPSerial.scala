import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{ SentencesAnnotation, TokensAnnotation }
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{ Annotation, StanfordCoreNLP }
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation

import scala.collection.JavaConversions._
import scala.xml.XML._

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
import java.io.Serializable

object sparkStanfordNLPSerial extends Serializable {

  //Sandy Ryza, Uri Laserson, Sean Owen, Josh Wills Advanced Analytics with Spark- Patterns for Learning from Data at Scale (2015)
  def plainTextToLemmas(text: String, stopWords: Set[String]): Seq[String] = {
    
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    var txtStr = text.toLowerCase()

   var txtStr1 = txtStr.toString().split("") // split on space
      .filter(_.nonEmpty) // remove empty string
      .map(_.replaceAll("""\W""", "") // remove special chars
        .toLowerCase).mkString(" ")

    println("spark::::::::::::" + txtStr1)

    val doc = new Annotation(txtStr1)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      //Remove Stop Words
      if (lemma.length > 2 && !stopWords.contains(lemma.toLowerCase())) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }
}