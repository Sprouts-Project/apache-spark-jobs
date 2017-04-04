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

case class customersegmentationage_and_brand(name: String, customerId: Int, usefullness: Int, numberOfReviews: Int)

object CustomerSegmentationAgeAndBrand extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid //Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    //Query to MySQL
    val customers = ReadMySQL.read("""(
   SELECT `digital-music`.customer.id as customer_id, `digital-music`.customer.birthdate,
       `digital-music`.item.brand,
       `digital-music`.ordereditem.quantity
FROM `digital-music`.ordereditem 
INNER JOIN `digital-music`.order_ ON `digital-music`.ordereditem.order_id = `digital-music`.order_.id
INNER JOIN `digital-music`.customer ON `digital-music`.customer.id = `digital-music`.order_.customer_id
INNER JOIN `digital-music`.item ON `digital-music`.item.id = `digital-music`.ordereditem.item_id
   ) AS data""", sqlContext).na.drop(Array("brand")).filter("brand != ''")

    val today = Calendar.getInstance().getTimeInMillis() / 1000 //current unix timestamp (seconds) 
    val conversion = 60 * 60 * 24 * 365 //age to seconds conversion

    val brandsIndexes = customers.select("brand").distinct().rdd.map(_.getString(0)).zipWithIndex()

    val mapBrandIndexes = sc.broadcast(brandsIndexes.collectAsMap.toMap)
    val mapReverseBrandIndexes = sc.broadcast(brandsIndexes.map(_.swap).collectAsMap().toMap)
    val nBrands = sc.broadcast(brandsIndexes.count().intValue())

    val customersTuples = customers.select(customers.col("customer_id"), unix_timestamp(customers.col("birthdate")), customers.col("brand"), customers.col("quantity"))
      .map { x => ((x.getInt(0), ((today - x.getLong(1)) / conversion).intValue()), (x.getString(2), x.getInt(3))) }

    //Map items to tupples (id, vector)
    val rddItemIdVectors = customersTuples.map {
      x =>
        (x._1, (mapBrandIndexes.value.get(x._2._1).head.intValue(), x._2._2)) //Map item (id, category_index)
    }
      .aggregateByKey(List[(Int, Int)]())(_ ++ List(_), _ ++ _) //Aggregate items, creating, for each item id, a list of categories indexes it belongs to
      .map {
        x =>
          (x._1._1, mapBrandsToFeatures(x._1._2, nBrands.value, x._2)) //Finally, map the list of categories indexes to a vector
      }

    val vectors = rddItemIdVectors.map(_._2) //gets the item vectors

    //Sets the K-Means algorithms up
    val numClusters = 5
    val numIterations = 20
    val clusters = KMeans.train(vectors, numClusters, numIterations)

    //val WSSSE = clusters.computeCost(vectors) //WSSSE error

    //Calculates the centroids. Here, we obtain the item profiles
    val centroids = sc.parallelize(clusters.clusterCenters)
      .map {
        x =>
          mapCentroidToItemProfile(mapReverseBrandIndexes.value, x) //For each centroid, which is a vector, we map the vector tot he category name it corresponds
      }
      .zipWithIndex() //Since Spark preserves the order, we obtain the index of each cluster
      .map(_.swap)
      .map { x => (x._1.intValue(), x._2) }
      .foreach(println)//Put first cluster index

    /*
    //DF to save in MongoDB
    val bestReviewers =
      sqlContext.createDataFrame(
      top20Reviewers.map {
          x =>
            BestReviewers(x._1._1,x._1._2, x._2._1,x._2._2) //Map each ellement in RDD with an ItemProfile
        }
      )

    //We finally persist the DF into MongoDB to extract it from the dashboard
    WriteMongoDB.deleteAndPersistDF(bestReviewers, sqlContext, "best_reviewers")
    bestReviewers.collect()
*/
  }

  //Method for mapping item categories, with consist in a list of indexes, to a Vector.
  def mapBrandsToFeatures(age: Int, brandSize: Int, brandTypes: List[(Int, Int)]): Vector = {
    var arr = Array.fill[Double](brandSize + 1)(0.0)

    for (x <- brandTypes) arr(x._1) += x._2
    arr(arr.size - 1) = age
    Vectors.dense(arr)
  }
  
   //Method for mapping centroids with item profiles
  def mapCentroidToItemProfile( nameMapper: Map[Long, String], vector: Vector): (Int,List[String]) = {
    val arrayVectors=vector.toArray
    val brandsArray=arrayVectors.slice(0,arrayVectors.size-1).zipWithIndex.filter(_._1 >= 0.1).map { x => nameMapper(x._2) }.toList
    (arrayVectors(arrayVectors.size-1).intValue(),brandsArray)
  }

}