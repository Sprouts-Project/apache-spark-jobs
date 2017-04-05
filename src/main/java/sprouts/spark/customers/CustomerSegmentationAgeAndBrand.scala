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

case class CustomerProfile(profile_id: Int, number_customers: Long, age_interval: String, brands: List[String])

object CustomerSegmentationAgeAndBrand extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    // Query to MySQL
    val customers = ReadMySQL.read("""(
   SELECT `digital-music`.customer.id as customer_id, `digital-music`.customer.birthdate,
       `digital-music`.item.brand,
       `digital-music`.ordereditem.quantity
FROM `digital-music`.ordereditem
INNER JOIN `digital-music`.order_ ON `digital-music`.ordereditem.order_id = `digital-music`.order_.id
INNER JOIN `digital-music`.customer ON `digital-music`.customer.id = `digital-music`.order_.customer_id
INNER JOIN `digital-music`.item ON `digital-music`.item.id = `digital-music`.ordereditem.item_id
   ) AS data""", sqlContext).na.drop(Array("brand")).filter("brand != ''")

    val today = Calendar.getInstance().getTimeInMillis() / 1000 // current unix timestamp (seconds)
    val conversion = 60 * 60 * 24 * 365 //   age to seconds conversion

    val brandsIndexes = customers.select("brand").distinct().rdd.map(_.getString(0)).zipWithIndex()

    val mapBrandIndexes = sc.broadcast(brandsIndexes.collectAsMap.toMap)
    val mapReverseBrandIndexes = sc.broadcast(brandsIndexes.map(_.swap).collectAsMap().toMap)
    val nBrands = sc.broadcast(brandsIndexes.count().intValue())

    val customersTuples = customers.select(customers.col("customer_id"), unix_timestamp(customers.col("birthdate")), customers.col("brand"), customers.col("quantity"))
      .map { x => ((x.getInt(0), ((today - x.getLong(1)) / conversion).intValue()), (x.getString(2), x.getInt(3))) }

    // Map items to tupples (id, vector)
    val rddCustomersVectors = customersTuples.map {
      x =>
        ((x._1._1, mapAgesToInterval(x._1._2.intValue())), (mapBrandIndexes.value.get(x._2._1).head.intValue(), x._2._2)) // Map (customer_id, age_interval) with values (brand index, quantity)
    }
      .aggregateByKey(List[(Int, Int)]())(_ ++ List(_), _ ++ _) // Aggregate customers, creating, for each (customer_id, age_interval), a list of (brand index, quantity)
      .map {
        x =>
          (x._1._1, mapBrandsToFeatures(x._1._2, nBrands.value, x._2)) // Finally, map the list of (brand_index, quantity) to a vector
      }

    val vectors = rddCustomersVectors.map(_._2) // Gets the customers vectors

    // Sets the K-Means algorithms up
    val numClusters = 5
    val numIterations = 100
    val clusters = KMeans.train(vectors, numClusters, numIterations)

    // val WSSSE = clusters.computeCost(vectors) //WSSSE error

    // Calculates the centroids. Here, we obtain the customers profiles
    val centroids = sc.parallelize(clusters.clusterCenters)
      .map {
        x =>
          mapCentroidToCustomerProfile(mapReverseBrandIndexes.value, x) // For each centroid, which is a vector, we map the vector to the brand and ange interval it corresponds
      }
      .zipWithIndex() // Since Spark preserves the order, we obtain the index of each cluster
      .map(_.swap)
      .map { x => (x._1.intValue(), x._2) }

    val customerProfilesDF = sqlContext.createDataFrame(rddCustomersVectors.map {
      x =>
        (x._1, clusters.predict(x._2)) // For each customer, we predict what cluster it belongs to
    }.map {
      x =>
        (x._2, 1) // We want to know how many customers belongs to each cluster, so we map cluster (x._2) to 1
    }.reduceByKey(_ + _) // And reduce by key (cluster index)
      .join(centroids) // Finally, we join this rdd with the centroids rdd so that we have (cluster_index, (number of customers, (age interval, list of brands)))
      .map { x => CustomerProfile(x._1, x._2._1, x._2._2._1, x._2._2._2) })

    WriteMongoDB.deleteAndPersistDF(customerProfilesDF, sqlContext, "customer_segmentation_age_and_brand")
    customerProfilesDF.collect()

  }

  // Method for mapping item categories, with consist in a list of indexes, to a Vector.
  def mapBrandsToFeatures(age: Int, brandSize: Int, brandTypes: List[(Int, Int)]): Vector = {
    var arr = Array.fill[Double](brandSize + 1)(0.0)

    for { x <- brandTypes } arr(x._1) += x._2

    // Let's normalize it
    val sum = arr.reduce(_ + _)
    if (sum > 0.0)
      for { i <- 0 until arr.size } arr(i) = arr(i) / sum

    arr(arr.size - 1) = age
    Vectors.dense(arr)
  }

  // Method for mapping centroids with item profiles
  def mapCentroidToCustomerProfile(nameMapper: Map[Long, String], vector: Vector): (String, List[String]) = {
    val arrayVectors = vector.toArray
    val vectorBrandsSize = vector.toArray.size - 1
    // val average = vector.toArray.slice(0, vectorBrandsSize).reduce(_+_)/vectorBrandsSize
    // println(average)
    // val brandsArray = arrayVectors.slice(0, arrayVectors.size - 1).zipWithIndex.filter(_._1 >= 0.1).map { x => nameMapper(x._2) }.toList
    val brandsArray = arrayVectors.slice(0, arrayVectors.size - 1).zipWithIndex.sortBy(_._1).reverse.take(7).map { x => nameMapper(x._2) }.toList // Returns the top 7 brands
    // val brandsArray = List(nameMapper.get(arrayVectors.slice(0, arrayVectors.size - 1).zipWithIndex.maxBy(_._1)._2.longValue()).get)

    (mapAgesIntervalsToString(arrayVectors(arrayVectors.size - 1).intValue()), brandsArray)
  }

  def mapAgesToInterval(age: Int): Int = {
    age match {
      case x if (x >= 16 && x < 20) => 0
      case x if (x >= 20 && x < 25) => 1
      case x if (x >= 25 && x < 30) => 2
      case x if (x >= 30 && x < 40) => 3
      case x if (x >= 40 && x < 50) => 4
      case x if (x >= 50 && x < 60) => 5
      case x if (x >= 60)           => 6
      case _                        => 99
    }
  }

  def mapAgesIntervalsToString(ageInterval: Int): String = {
    ageInterval match {
      case 0 => "16-19"
      case 1 => "20-24"
      case 2 => "25-29"
      case 3 => "30-39"
      case 4 => "40-49"
      case 5 => "50-59"
      case 6 => "more than 60"
      case _ => "Other"
    }
  }

}
