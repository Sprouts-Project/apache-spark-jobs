package sprouts.spark.customers

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.typesafe.config.Config

import spark.jobserver.SparkJob
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import sprouts.spark.utils.DBProperties
import com.mongodb.MongoCredential
import com.mongodb.MongoClientURI
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import sprouts.spark.utils.WriteMongoDB
import sprouts.spark.utils.ReadMySQL

case class BestReviewers(name: String, customerId: Int, usefullness: Int, numberOfReviews: Int)

object BestReviewers extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    // Query to MySQL
    val reviewers = ReadMySQL.read("(SELECT customer.name,customer.id,review.helpful,review.unhelpful FROM review inner join customer on review.customer_id =customer.id) AS data", sqlContext)

    val top20Reviewers = reviewers.select(reviewers.col("name"), reviewers.col("id"), reviewers.col("helpful"), reviewers.col("unhelpful"))
      .map { x => ((x.getString(0), x.getInt(1)), ((x.getInt(2) - x.getInt(3)), 1)) }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .sortBy(_._2._1, false)
      .take(20)

    // DF to save in MongoDB
    val bestReviewers =
      sqlContext.createDataFrame(
        top20Reviewers.map {
          x =>
            BestReviewers(x._1._1, x._1._2, x._2._1, x._2._2) // Map each ellement in RDD with an ItemProfile
        })

    // We finally persist the DF into MongoDB to extract it from the dashboard
    WriteMongoDB.deleteAndPersistDF(bestReviewers, sqlContext, "best_reviewers")
    bestReviewers.collect()

  }
}
