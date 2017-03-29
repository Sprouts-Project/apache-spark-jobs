package sprouts.spark.recommender

import spark.jobserver.SparkJob
import spark.jobserver.SparkJobValid
import org.apache.spark.SparkContext
import spark.jobserver.SparkJobValidation
import org.apache.spark.sql.SQLContext
import com.typesafe.config.Config
import sprouts.spark.utils.ReadMySQL
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import sprouts.spark.utils.WriteMongoDB

object CollaborativeFiltering extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid //Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    val reviewsDf = ReadMySQL.read("(SELECT customer_id, item_id, overall FROM review) as data", sqlContext) //load reviews from MySQL

    val ratings = reviewsDf.map { x => //maps each review to Rating object
      Rating(x.getInt(0).intValue(),
        x.getInt(1).intValue(),
        x.getDouble(2))
    }

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01) //creates the model

    //val modelBroadcast = sc.broadcast(model)

    model.save(sc, "/data/jobserver/models")

  }
}