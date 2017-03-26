package sprouts.spark.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame

object WriteMongoDB {

  def deleteAndPersistDF(dataframe: DataFrame, sqlContext: SQLContext, collection: String) = {
    deleteCollection(collection)
    persistDF(dataframe, sqlContext, collection)
  }

  def deleteCollection(collection: String) = {
    val mongoUri = new MongoClientURI(DBProperties.mongodbUri)
    new MongoClient(mongoUri).getDatabase(mongoUri.getDatabase).getCollection(collection).deleteMany(new BasicDBObject())
  }

  def persistDF(dataframe: DataFrame, sqlContext: SQLContext, collection: String) = {
    MongoSpark.save(dataframe, WriteConfig(Map("collection" -> collection), Some(WriteConfig(sqlContext))))
  }
}