
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
object generateNgramsSerial extends Serializable {
  //Generate Bigrams
  def bigrams(s: RDD[List[String]]) {
    //Split the string based on " " 
    //TODO:use filter function to use stemming
    val rdd = s.map(x => x.toString().split(", ") // split on space
      .filter(_.nonEmpty) // remove empty string
      .map(_.replaceAll("""\W""", "") // remove special chars
          )
      .filter(_.nonEmpty) //Remove Empty splits
      //.map(stemmLemma)
      .sliding(2) // take continuous pairs
      .filter(_.size == 2) // sliding can return partial
      .map { case Array(a, b) => ((a, b), 1) }).flatMap(x => x).countByKey()
      .foreach { case ((x, y), z) => if (z > 1) println(s"('${x}', '${y}') ${z}") }

  }
}