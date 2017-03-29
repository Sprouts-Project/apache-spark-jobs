package sprouts.spark.items

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

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

case class ItemProfile(profile_id: Int, number_items: Long, categories: List[String])

object ItemProfiles extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid //Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    //Query to MySQL
    val categories = ReadMySQL.read("(SELECT id, name FROM category) AS data", sqlContext).rdd.map { x => (x.getInt(0), x.getString(1)) }
    val itemCats = ReadMySQL.read("(SELECT * FROM item_category) AS data", sqlContext)

    //Map categories with unique index and broadcast through SparkContext
    val categoriesIds = categories.map { x => x._1 }.zipWithIndex()
    val mapCatIdToIndex = sc.broadcast(categoriesIds.collectAsMap.toMap)
    val mapIndexToCatId = sc.broadcast(categoriesIds.map(_.swap).collectAsMap().toMap)

    //Map category id with its name
    val mapCatIdToStr = sc.broadcast(categories.collectAsMap().toMap)
    //Gets the number of categories
    val nCategories = sc.broadcast(categoriesIds.count().intValue())

    //Map items to tupples (id, vector)
    val rddItemIdVectors = itemCats.rdd.map {
      x =>
        (x.getInt(0), mapCatIdToIndex.value.get(x.getInt(1)).head.intValue()) //Map item (id, category_index)
    }
      .aggregateByKey(List[Int]())(_ ++ List(_), _ ++ _) //Aggregate items, creating, for each item id, a list of categories indexes it belongs to
      .map {
        x =>
          (x._1, mapItemToFeatures(nCategories.value, x._2)) //Finally, map the list of categories indexes to a vector
      }

    val vectors = rddItemIdVectors.map(_._2) //gets the item vectors

    //Sets the K-Means algorithms up
    val numClusters = 150
    val numIterations = 20
    val clusters = KMeans.train(vectors, numClusters, numIterations)

    //val WSSSE = clusters.computeCost(vectors) //WSSSE error

    //Calculates the centroids. Here, we obtain the item profiles
    val centroids = sc.parallelize(clusters.clusterCenters)
      .map {
        x =>
          mapCentroidToItemProfile(mapIndexToCatId.value, mapCatIdToStr.value, x) //For each centroid, which is a vector, we map the vector tot he category name it corresponds
      }
      .zipWithIndex() //Since Spark preserves the order, we obtain the index of each cluster
      .map(_.swap)
      .map { x => (x._1.intValue(), x._2) } //Put first cluster index

    //amount of items for each profile
    val numItemsByProfile = rddItemIdVectors.map {
      x =>
        (x._1, clusters.predict(x._2)) // For each item, we predict what cluster it belongs to
    }.map {
      x =>
        (x._2, 1) //We want to know how many items belongs to each cluster, so we map cluster (x._2) to 1
    }
      .reduceByKey(_ + _) //And reduce by key (cluster index)
      .join(centroids) //Finally, we join this rdd with the centroids rdd so that we have (cluster_index, (number of items, list of categories names))

    //DF to save in MongoDB
    val itemProfile =
      sqlContext.createDataFrame(
        numItemsByProfile.map {
          x =>
            ItemProfile(x._1, x._2._1.longValue(), x._2._2) //Map each ellement in RDD with an ItemProfile
        })

    //We finally persist the DF into MongoDB to extract it from the dashboard
    WriteMongoDB.deleteAndPersistDF(itemProfile, sqlContext, "item_profiles")
    itemProfile.collect()
  }

  //Method for mapping item categories, with consist in a list of indexes, to a Vector.
  def mapItemToFeatures(catSize: Int, itemCategories: List[Int]): Vector = {
    var arr = Array.fill[Double](catSize)(0.0)

    for (x <- itemCategories) arr(x) = 1.0
    Vectors.dense(arr)
  }

  //Method for mapping centroids with item profiles
  def mapCentroidToItemProfile(indexMapper: Map[Long, Int], nameMapper: Map[Int, String], vector: Vector): List[String] = {
    vector.toArray.zipWithIndex.filter(_._1 >= 0.5).map { x => nameMapper(indexMapper.get(x._2.longValue()).get) }.toList
  }

}