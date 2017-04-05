package sprouts.spark.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

object ReadMySQL {
  def read(query: String, sqlContext: SQLContext): DataFrame = {
    sqlContext.read.jdbc(DBProperties.jdbcUrl, query, DBProperties.mySqlProperties)
  }
}
