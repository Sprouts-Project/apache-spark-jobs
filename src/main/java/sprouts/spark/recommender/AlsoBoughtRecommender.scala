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

case class AlsoBoughtRecommender(item_id: Int, alsoBought: List[RecommendedAndQuantity])
case class RecommendedAndQuantity(item_id: Int, title: String, brand: String, imUrl:String, quantity: Int)

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
    val df = ReadMySQL.read("""(SELECT ordereditem.item_id , ordereditem.order_id ,ordereditem.quantity, item.title, item.brand, item.imUrl
        FROM `digital-music`.ordereditem
        INNER JOIN item ON ordereditem.item_id = item.id
       ) AS data""", sqlContext)
    // self join each item on the corresponding ordereditem
    // then, it groups by pairs item_id, r_item_id, and finally aggregate by sum of quantities
    // as a result, it returns item_id, r_item_id, and sum of quantities
    val results = df.join(df.select(col("item_id").alias("r_item_id"), col("order_id").alias("r_order_id"), col("quantity").alias("weight"),
        col("title").alias("r_title"),col("brand").alias("r_brand"),col("imUrl").alias("r_imUrl")), col("order_id") === col("r_order_id") and col("item_id").notEqual(col("r_item_id")))
      .groupBy(
        col("item_id"), col("r_item_id"),col("r_title"),col("r_brand"),col("r_imUrl")).agg(
           sum(col("weight")))
    
    // for each item, aggregate by item id, having as value the recommended item and a list indicating the total quantity also bought
    val recommend = results.select(results.col("item_id"), results.col("r_item_id"),results.col("r_title"),results.col("r_brand"),results.col("r_imUrl"), results.col("sum(weight)"))
      .map { x => (x.getInt(0), RecommendedAndQuantity(x.getInt(1),x.getString(2),x.getString(3),x.getString(4), x.getLong(5).toInt)) }
      .aggregateByKey(List[RecommendedAndQuantity]())(_ ++ List(_), _ ++ _)

    // DF to save in MongoDB
    val alsoB =
      sqlContext.createDataFrame(
        recommend.map {
          x =>
            AlsoBoughtRecommender(x._1, x._2)
        })

    // We finally persist the DF into MongoDB to extract it from the dashboard
    WriteMongoDB.deleteAndPersistDF(alsoB, sqlContext, "also_bought_recommender")
  }
}