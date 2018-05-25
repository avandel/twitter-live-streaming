package com.avandel.twitter

import com.avandel.twitter.MetricType.MetricType
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser
import twitter4j.{Status, URLEntity}
import com.mongodb.spark.sql._

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scalaj.http.{Http, HttpOptions}

object MetricType extends Enumeration {
  type MetricType = Value
  val WORD, LINK = Value
}

case class MetricCount(_id: String, count: Long, metricType: String)

object Twitter {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  case class CLIParams(checkpointDirectory: String = "./checkpointDirectory", filters: Array[String] = Array.empty, batchDuration: Long = 10l, mongoOutputUri: String = "mongodb://127.0.0.1/test.metriccount")

  def parseArgs(appName: String): OptionParser[CLIParams] = {
    new OptionParser[CLIParams](appName) {
      head(appName, "1.0")
      help("help") text "prints this usage text"

      opt[String]("batchDuration") required() action { (data, conf) =>
        conf.copy(batchDuration = data.toLong)
      } text "Batch duration in second. Example : 600 (for 10 min)"

      opt[String]("checkpointDirectory") required() action { (data, conf) =>
        conf.copy(checkpointDirectory = data)
      } text "Directory for checkpointing."

      opt[String]("mongoOutputUri") required() action { (data, conf) =>
        conf.copy(mongoOutputUri = data)
      } text "Output uri for writing to MongoDB. Example :  mongodb://127.0.0.1/test.metriccount"

      opt[String]("filters") required() action { (data, conf) =>
        conf.copy(filters = conf.filters :+ data)
      } text "Filters to use twitter api. Example : test, twitter"

    }
  }

  /**
    *
    * @param status
    * @return all external links from a status recursively.
    *
    */
  def resolveExternalLinksFromStatus(status: Status): Seq[String] = {

    val entities: Array[URLEntity] = status.getURLEntities
    val pattern = """https://twitter.com/.+""".r

    entities
      .map(entity => getFinalUrl(entity.getExpandedURL))
      .flatMap {
        case Some(url) => url match {
          case pattern() if status.isRetweeted => resolveExternalLinksFromStatus(status.getRetweetedStatus)
          case pattern() if status.getQuotedStatus != null => resolveExternalLinksFromStatus(status.getQuotedStatus)
          case pattern() => Seq.empty[String]
          case _ => Seq(url)
        }
        case None => Seq.empty[String]
      }
  }

  /**
    *
    * @param url
    * @return the final url after following the successive redirects if the last get call is a sucess.
    */
  @tailrec
  def getFinalUrl(url: String): Option[String] = {

    val response = Try(
      Http(url)
        .timeout(connTimeoutMs = 2000, readTimeoutMs = 5000)
        .options(HttpOptions.followRedirects(false))
        .asString
    )

    response match {
      case Success(resp) =>
        resp.header("Location") match {
          case Some(location) if resp.isRedirect => getFinalUrl(location)
          case None if resp.isSuccess => Some(url)
          case _ => None
        }
      case Failure(_) => None
    }

  }

  def createContext(appName: String, config: CLIParams): StreamingContext = {
    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    logger.debug("Setup the Streaming context")

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", "local[3]")

    // Create the context with a 10 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(config.batchDuration))
    ssc.checkpoint(config.checkpointDirectory)
    ssc
  }

  def main(args: Array[String]): Unit = {

    val parser = parseArgs("Twitter")

    parser.parse(args, CLIParams()) match {
      case Some(params) =>

        val config = ConfigFactory.load()
        val consumerKey = config.getString("twitter4j.oauth.consumerKey")
        val consumerSecret = config.getString("twitter4j.oauth.consumerSecret")
        val accessToken = config.getString("twitter4j.oauth.accessToken")
        val accessTokenSecret = config.getString("twitter4j.oauth.accessTokenSecret")

        // Set the system properties so that Twitter4j library used by twitter stream
        // can use them to generat OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
        System.setProperty("twitter4j.oauth.accessToken", accessToken)
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

        implicit val ssc: StreamingContext = createContext("TwitterZen", params)

        SparkTwitter(params).processTweets()

        ssc.start()
        ssc.awaitTermination()
      case None =>
        logger.error("arguments are bad, error message will have been displayed")
        parser.showUsageAsError
    }
  }


  case class SparkTwitter(params: CLIParams) {

    def processTweets()(implicit ssc: StreamingContext): Unit = {

      val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, params.filters)

      processWordTweets(stream)
      processLinkTweets(stream)
    }

    def stateUpdateFunction(word: String, one: Option[Long], state: State[Long]): (String, Long) = {
      val sum = one.getOrElse(0L) + state.getOption.getOrElse(0L)
      val output = (word, sum)
      state.update(sum)
      output
    }

    private def processWordTweets(stream: ReceiverInputDStream[Status]): Unit = {

      val wordDstream: DStream[(String, Long)] = stream
        .filter(_.getURLEntities.isEmpty)
        .flatMap(_.getText.split("\\s+"))
        .map(word => (word, 1L))

      wordDstream.mapWithState(StateSpec.function(stateUpdateFunction _)).foreachRDD({
        rdd =>
          saveMetricTweetsToMongoDB(rdd, MetricType.WORD)
      })
    }

    private def processLinkTweets(stream: ReceiverInputDStream[Status]): Unit = {

      val urlDStream: DStream[(String, Long)] = stream
        .filter(!_.getURLEntities.isEmpty)
        .flatMap(tweet => Twitter.resolveExternalLinksFromStatus(tweet))
        .map(url => (url, 1L))

      urlDStream.mapWithState(StateSpec.function(stateUpdateFunction _)).foreachRDD({
        rdd =>
          saveMetricTweetsToMongoDB(rdd, MetricType.LINK)
      })
    }

    private def saveMetricTweetsToMongoDB(rdd: RDD[(String, Long)], metricType : MetricType): Unit = {

      val spark = SparkSession.builder.config("spark.mongodb.output.uri", params.mongoOutputUri).getOrCreate()
      import spark.implicits._
      val urlCounts = rdd.map({ case (metric: String, count: Long) => MetricCount(metric, count, metricType.toString) }).toDF()
      urlCounts.write.mode(SaveMode.Append).mongo()
    }
  }

}
