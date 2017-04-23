package sprouts.spark.customers

import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import com.typesafe.config.Config

import spark.jobserver.SparkJob
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import sprouts.spark.utils.ReadMongoDB
import sprouts.spark.utils.ReadMySQL
import org.apache.spark.mllib.clustering.KMeans
import sprouts.spark.utils.WriteMongoDB

case class CustomerProfileAgeAndItemProfile(profile_id: Int, number_customers: Int, age_interval: String, categories: List[String])

object CustomerSegmentationAgeAndItemProfile extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {

    val sqlContext = SQLContext.getOrCreate(sc)

    val customers = ReadMySQL.read("""(
   SELECT `digital-music`.customer.id as customer_id, `digital-music`.customer.birthdate,
       `digital-music`.ordereditem.item_id,
       `digital-music`.ordereditem.quantity
FROM `digital-music`.ordereditem
INNER JOIN `digital-music`.order_ ON `digital-music`.ordereditem.order_id = `digital-music`.order_.id
INNER JOIN `digital-music`.customer ON `digital-music`.customer.id = `digital-music`.order_.customer_id
   ) AS data""", sqlContext)

    val itemProfiles = ReadMongoDB.read(sqlContext, "item_profile_item_id_map")
      .select("categories", "item_id", "profile_id")
        
    val customersItemProf = customers.join(itemProfiles, "item_id")

    val today = Calendar.getInstance().getTimeInMillis() / 1000 // current unix timestamp (seconds)
    val conversion = 60 * 60 * 24 * 365 // age to seconds conversion

    val mapItemProfileToCategories = sc.broadcast(itemProfiles.map { x => (x.getInt(2), x.getAs[List[String]](0).toArray) }.distinct().collectAsMap().toMap)
    val nItemProfiles = sc.broadcast(mapItemProfileToCategories.value.keys.size)

    val customerTuples = customersItemProf.select(customersItemProf.col("customer_id"), unix_timestamp(customersItemProf.col("birthdate")), customersItemProf.col("profile_id"), customersItemProf.col("quantity"))
      .map { x => ((x.getInt(0), mapAgesToInterval(((today - x.getLong(1)) / conversion).intValue)), (x.getInt(2), x.getInt(3))) } // get ((customer_id, age), (profile_id, quantity))

    val rddCustomersVectors = customerTuples
      .aggregateByKey(List[(Int, Int)]())(_ ++ List(_), _ ++ _) // Aggregate customers, creating, for each (customer_id, age_interval), a list of (item profile, quantity)
      .map {
        x =>
          (x._1._1, mapCustomerToFeatures(x._1._2, nItemProfiles.value, x._2)) // Finally, map the list of (item_profile, quantity) to a vector
      }

    val vectors = rddCustomersVectors.map(_._2) // gets the item vectors

    // Sets the K-Means algorithms up
    val numClusters = 5
    val numIterations = 20
    val clusters = KMeans.train(vectors, numClusters, numIterations)

    // val WSSSE = clusters.computeCost(vectors) //WSSSE error

    // Calculates the centroids. Here, we obtain the customer profiles
    val centroids = sc.parallelize(clusters.clusterCenters)
      .map {
        x =>
          mapCentroidToCustomerProfile(mapItemProfileToCategories.value, x) // For each centroid, which is a vector, we map the vector to the brand and ange interval it corresponds
      }
      .zipWithIndex() // Since Spark preserves the order, we obtain the index of each cluster
      .map(_.swap)
      .map { x => (x._1.intValue(), x._2) }

    val customerProfilesDF = sqlContext.createDataFrame(rddCustomersVectors.map {
      x => (x._1, clusters.predict(x._2)) // for each customer predict cluster
    }.map {
      x =>
        (x._2, 1)
    }.reduceByKey(_ + _)
      .join(centroids)
      .map { x => CustomerProfileAgeAndItemProfile(x._1, x._2._1, x._2._2._1, x._2._2._2) })

    WriteMongoDB.deleteAndPersistDF(customerProfilesDF, sqlContext, "customer_segmentation_age_and_item_profile")
    customerProfilesDF.collect().foreach(println)

  }

  def mapCustomerToFeatures(age: Int, itemProfilesSize: Int, itemProfiles: List[(Int, Int)]): Vector = {
    var arr = Array.fill[Double](itemProfilesSize + 1)(0.0)

    for {x <- itemProfiles} arr(x._1) += x._2

    // Let's normalize it
    val sum = arr.reduce(_ + _)
    if (sum > 0.0)
      for {i <- 0 until arr.size} arr(i) = arr(i) / sum

    arr(arr.size - 1) = age
    Vectors.dense(arr)
  }

  // Method for mapping centroids with customer profiles
  def mapCentroidToCustomerProfile(nameMapper: Map[Int, Array[String]], vector: Vector): (String, List[String]) = {
    val arrayVectors = vector.toArray

    // max value
    val profilesArray = Array(nameMapper.get(arrayVectors.slice(0, arrayVectors.size - 1).zipWithIndex.maxBy(_._1)._2.intValue).get).flatten.toList

    (mapAgesIntervalsToString(arrayVectors(arrayVectors.size - 1).intValue()), profilesArray)
  }

  def mapAgesToInterval(age: Int): Int = {
    age match {
      case x if (x >= 16 && x < 22) => 0
      case x if (x >= 22 && x < 30) => 1
      case x if (x >= 30 && x < 50) => 2
      case x if (x >= 50)           => 3
      case _                        => 99
    }
  }

  def mapAgesIntervalsToString(ageInterval: Int): String = {
    ageInterval match {
      case 0 => "16-21"
      case 1 => "22-29"
      case 2 => "30-49"
      case 3 => "more than 50"
      case _ => "Other"
    }
  }
}
