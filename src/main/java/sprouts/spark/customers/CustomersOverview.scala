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

case class CustomerOverview(average_age: Double, average_age_male: Double, average_age_female: Double, average_age_by_state: Array[(String, Double)], customers_by_state: Array[(String, Int)])

object CustomersOverview extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid //Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    //Query to MySQL
    val customers = ReadMySQL.read("(SELECT * FROM customer) AS data", sqlContext)

    val today = Calendar.getInstance().getTimeInMillis() / 1000 //current unix timestamp (seconds) 
    val conversion = 60 * 60 * 24 * 365 //age to seconds conversion

    val customersAge = customers.select(unix_timestamp(customers.col("birthdate")))
      .map { x => (((today - x.getLong(0)) * 1.0 / conversion)) }

    val averageAge = customersAge.mean()

    val customersAgeMale = customers.select(unix_timestamp(customers.col("birthdate")), customers.col("sex")).filter(customers.col("sex") === "M")
      .map { x => (((today - x.getLong(0)) * 1.0 / conversion)) }

    val averageAgeMale = customersAgeMale.mean()

    val customersAgeFemale = customers.select(unix_timestamp(customers.col("birthdate")), customers.col("sex")).filter(customers.col("sex") === "F")
      .map { x => (((today - x.getLong(0)) * 1.0 / conversion)) }

    val averageAgeFemale = customersAgeFemale.mean()

    val customersByState = customers.select(customers.col("state"))
      .map { x => (x.getString(0), 1) }
      .reduceByKey(_ + _)
      .collect()

    val averageAgeByState = customers.select(customers.col("state"), unix_timestamp(customers.col("birthdate")))
      .map { x => (x.getString(0), ((((today - x.getLong(1)) * 1.0 / conversion), 1))) }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues { case (sum, count) => sum / count }
      .collect()

    //DF to save in MongoDB
    val customerOverview =
      sqlContext.createDataFrame(List(CustomerOverview(averageAge, averageAgeMale, averageAgeFemale, averageAgeByState, customersByState)))

    //We finally persist the DF into MongoDB to extract it from the dashboard
    WriteMongoDB.deleteAndPersistDF(customerOverview, sqlContext, "customer_overview")
    customerOverview.collect()

  }
}