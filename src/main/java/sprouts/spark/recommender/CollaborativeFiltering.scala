package sprouts.spark.recommender

import java.io.IOException

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SQLContext

import com.typesafe.config.Config

import spark.jobserver.SparkJob
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import sprouts.spark.utils.ReadMySQL
import scalax.file.Path

object CollaborativeFiltering extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    val reviewsDf = ReadMySQL.read("(SELECT customer_id, item_id, overall FROM review) as data", sqlContext) // load reviews from MySQL

    val ratings = reviewsDf.map { x => // maps each review to Rating object
      Rating(x.getInt(0).intValue(),
        x.getInt(1).intValue(),
        x.getDouble(2))
    }

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01) // creates the model

    // val modelBroadcast = sc.broadcast(model)

    val path = Path.fromString("/data/jobserver/models") // TODO: this path could be passed by config file
    try {
      path.deleteRecursively(continueOnFailure = false)
    } catch {
      case e: IOException => // some file could not be deleted
    }

    model.save(sc, "/data/jobserver/models")

  }
}
