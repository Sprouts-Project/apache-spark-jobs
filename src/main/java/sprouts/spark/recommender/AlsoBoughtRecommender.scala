package sprouts.spark.recommender

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
import org.apache.spark.sql.functions._

case class AlsoBoughtRecommender(itemID: Int, alsoBought: List[(Int, Long)])

object AlsoBoughtRecommender extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    // Query to MySQL
    val df = ReadMySQL.read("""(SELECT ordereditem.item_id , ordereditem.order_id ,ordereditem.quantity
        FROM `digital-music`.ordereditem 
       ) AS data""", sqlContext)

    val results = df.join(df.select(col("item_id").alias("r_item_id"), col("order_id").alias("r_order_id"), col("quantity").alias("weight")), col("order_id") === col("r_order_id") and col("item_id").notEqual(col("r_item_id")))
      .groupBy(
        col("item_id"), col("r_item_id")).agg(
          col("item_id"), col("r_item_id"), sum(col("weight")))

    print("Primera fase")
    val pri = results.select(results.col("item_id"), results.col("r_item_id"), results.col("sum(weight)"))
      .map { x => (x.getInt(0), (x.getInt(1), x.getLong(2))) }
      .aggregateByKey(List[(Int, Long)]())(_ ++ List(_), _ ++ _)

    // DF to save in MongoDB
    val test =
      sqlContext.createDataFrame(
        pri.map {
          x =>
            AlsoBoughtRecommender(x._1, x._2)
        })

    // We finally persist the DF into MongoDB to extract it from the dashboard
    WriteMongoDB.deleteAndPersistDF(test, sqlContext, "also_bought_recommender")
    test.collect()

  }
}