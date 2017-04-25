package sprouts.spark.recommender

import spark.jobserver.SparkJob
import spark.jobserver.SparkJobValid
import org.apache.spark.SparkContext
import sprouts.spark.utils.ReadMySQL
import spark.jobserver.SparkJobValidation
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.recommendation.ALS
import com.typesafe.config.Config
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import sprouts.spark.utils.WriteMongoDB

case class CollaborativeFilteringRecommendation(customer_id:Int, items:List[Item])
//case class RecommendedItem(id:Int, title:String, brand:String, imUrl:String, price:Double)

object RecommendProductsCollaborativeFiltering extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val user = jobConfig.getString("input.string").toInt
    execute(sc, user)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }
  
  def execute(sc: SparkContext, user: Int): Any = {
    val model = MatrixFactorizationModel.load(sc, "/data/jobserver/models")
    val recommendations = model.recommendProducts(user, 20)

    val sqlContext = SQLContext.getOrCreate(sc)

    val recommendationsDf = sqlContext.createDataFrame(recommendations)

    // return recommendations

    val query = recommendationsDf.filter("rating>=3.0").map { x => x.getInt(1) }.collect.mkString(" OR id = ")
    
    val itemsDf = sqlContext.createDataFrame(
      ReadMySQL.read("(SELECT id, title, brand, imUrl, price FROM item WHERE id ="+query+") AS data", sqlContext)
      .map { x => (user, Item(x.getInt(0), x.getString(1), x.getString(2), x.getString(3), x.getDouble(4))) }
      .aggregateByKey(List[Item]())(_ ++ List(_), _ ++ _)
      .map{ x => CollaborativeFilteringRecommendation(x._1, x._2) }
    )
    // save the recommendations to the warehouse collection collaborative_filtering_recommendations
    WriteMongoDB.persistDF(itemsDf, sqlContext, "collaborative_filtering_recommendations")
    
    itemsDf.toJSON.collect
  }
}
