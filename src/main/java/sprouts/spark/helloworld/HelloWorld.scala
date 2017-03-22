package sprouts.spark.helloworld

import spark.jobserver.SparkJob
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobValid
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf

object HelloWorld extends SparkJob {

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    sc.parallelize(jobConfig.getString("input.string").split(" ").toSeq).countByValue
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid //Always valid
  }
}