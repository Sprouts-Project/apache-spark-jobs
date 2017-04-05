package sprouts.spark.utils

import java.io.File

import com.typesafe.config.ConfigFactory
import java.util.Properties

object DBProperties {
  // Load config from /data/jobserver/conf/dbconfig.conf
  val dbconfig = ConfigFactory.parseFile(new File("/data/jobserver/conf/dbconfig.conf"))

  // MySQL JDBC url
  val jdbcUrl = "jdbc:mysql://" + dbconfig.getString("mysql.host") + ":" + dbconfig.getString("mysql.port") + "/" + dbconfig.getString("mysql.db")

  // MySQL properties
  val mySqlProperties = new Properties()
  mySqlProperties.setProperty("user", DBProperties.dbconfig.getString("mysql.user"))
  mySqlProperties.setProperty("password", DBProperties.dbconfig.getString("mysql.password"))
  mySqlProperties.put("driver", DBProperties.dbconfig.getString("mysql.driver"))

  // MongoDB URI
  val mongodbUri = dbconfig.getString("mongodb.mongodbUri")
}
