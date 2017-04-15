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

case class AlsoBoughtRecommender(int1:Int,int2:Int,int3:Long)

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
    val df = ReadMySQL.read("""(SELECT ordereditem.item_id , ordereditem.order_id 
        FROM `digital-music`.ordereditem 
        where ordereditem.item_id=116720 or ordereditem.item_id=116534
        
       ) AS data""", sqlContext)
      
    val df2 = ReadMySQL.read("""(SELECT ordereditem.item_id as r_item_id, ordereditem.order_id as r_order_id, ordereditem.quantity
        FROM `digital-music`.ordereditem 
        where ordereditem.item_id=116534 or ordereditem.item_id=116720
       ) AS data""", sqlContext)
    val results = df.join(df2,df.col("order_id") === df2.col("r_order_id") and df.col("item_id").notEqual(df2.col("r_item_id")))
    .groupBy(
  df.col("item_id"), df2.col("r_item_id")
).agg(
  df.col("item_id"),df2.col("r_item_id"), sum(df2.col("quantity"))
)
      
      print("Primera fase")
    val pri=results.select(results.col("item_id"),results.col("r_item_id"),results.col("sum(quantity)"))
    .map{  x => (x.getInt(0),x.getInt(1),x.getLong(2))}
    
    
    
    
     // DF to save in MongoDB
    val test =
      sqlContext.createDataFrame(
        pri.map {
          x =>
            AlsoBoughtRecommender(x._1,x._2,x._3) // Map each ellement in RDD with an ItemProfile
        })

    // We finally persist the DF into MongoDB to extract it from the dashboard
    WriteMongoDB.deleteAndPersistDF(test, sqlContext, "newtest")
    test.collect()
    
      

  }
}