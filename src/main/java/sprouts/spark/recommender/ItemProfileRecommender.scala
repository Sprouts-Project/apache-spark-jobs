package sprouts.spark.recommender

import spark.jobserver.SparkJob
import spark.jobserver.SparkJobValid
import org.apache.spark.SparkContext
import spark.jobserver.SparkJobValidation
import org.apache.spark.sql.SQLContext
import com.typesafe.config.Config
import sprouts.spark.utils.ReadMySQL
import sprouts.spark.utils.ReadMongoDB
import sprouts.spark.utils.WriteMongoDB

case class Item(id:Int, title:String, brand:String, imUrl:String, price:Double)
case class ItemProfileRecommender(item_id:Int, categories:List[String], items:List[Item])

object ItemProfileRecommender extends SparkJob {
    override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)
    
    val itemIds = ReadMySQL.read("(SELECT id FROM item) as DATA", sqlContext)
    val itemProfileItem = ReadMongoDB.read(sqlContext, "item_profile_item_id_map")
      .select("profile_id", "item_id", "item_title", "item_description", "item_brand", "item_imUrl", "item_price", "categories")

    val itemProfileItemsMap = sc.broadcast(itemProfileItem
      .map { x => (x.getInt(0), Item(x.getInt(1), x.getString(2), x.getString(4), x.getString(5), x.getDouble(6))) } // key: item_profile, value: item
      .aggregateByKey(List[Item]())(_ ++ List(_), _ ++ _)
      .collectAsMap.toMap)
    
    val itemProfileRecommender = sqlContext.createDataFrame(itemProfileItem
      .map { x => ItemProfileRecommender(
          x.getInt(1),
          x.getAs[Seq[String]](7).toList,
          itemProfileItemsMap.value.get(x.getInt(0)).get
      ) })
      
    WriteMongoDB.deleteAndPersistDF(itemProfileRecommender, sqlContext, "item_profile_recommender")
    
  }
}