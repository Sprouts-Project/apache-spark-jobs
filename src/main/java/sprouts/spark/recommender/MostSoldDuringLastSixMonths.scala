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

case class top50SoldDuringLastSixMonths(item_id:Int, item_brand:String, item_description:String, item_imUrl:String, item_price:Double, item_title:String, quantity:Int)

object MostSoldDuringLAstSixMonths extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    // Query to MySQL
    val soldDuringLastSix = ReadMySQL.read("""(SELECT item.*,ordereditem.quantity
FROM ordereditem
INNER JOIN `digital-music`.order_ ON `digital-music`.ordereditem.order_id=`digital-music`.order_.id
INNER JOIN `digital-music`.item ON `digital-music`.ordereditem.item_id=`digital-music`.item.id
AND order_.date >= DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-%d'), INTERVAL 6 MONTH)
AND order_.date < DATE_FORMAT(NOW() ,'%Y-%m-%d')) AS data""", sqlContext)

    val top50MostSoldDuringLastSix = soldDuringLastSix.select(soldDuringLastSix.col("id"),
        soldDuringLastSix.col("brand"),soldDuringLastSix.col("description"),soldDuringLastSix.col("imUrl"),
        soldDuringLastSix.col("price"),soldDuringLastSix.col("title"),soldDuringLastSix.col("quantity"))
      .map { x => ((x.getInt(0),x.getString(1),x.getString(2),x.getString(3),x.getDouble(4),x.getString(5)),x.getInt(6)) }
    .reduceByKey(_ + _)
    .sortBy(_._2, false) // Sort descending by value
    .take(50)

// DF to save in MongoDB
    val mostSoldDuringLastSix =
      sqlContext.createDataFrame(
        top50MostSoldDuringLastSix.map {
          x =>
           top50SoldDuringLastSixMonths(x._1._1,x._1._2,x._1._3,x._1._4,x._1._5,x._1._6,x._2) // Map each ellement in RDD with an ItemProfile
        })

    // We finally persist the DF into MongoDB to extract it from the dashboard
    WriteMongoDB.deleteAndPersistDF(mostSoldDuringLastSix, sqlContext, "most_sold_during_last_six_months")
    mostSoldDuringLastSix.collect()
  }
}
