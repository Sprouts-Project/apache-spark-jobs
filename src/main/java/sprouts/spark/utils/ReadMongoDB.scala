package sprouts.spark.utils

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.config.ReadConfig

object ReadMongoDB {
  
  def read(sqlContext: SQLContext, collection: String): DataFrame = {
    MongoSpark.load(sqlContext, ReadConfig(Map("collection" -> collection), Some(ReadConfig(sqlContext))))
  }
}
