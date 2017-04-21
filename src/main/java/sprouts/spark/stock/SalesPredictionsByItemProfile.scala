package sprouts.spark.stock

import spark.jobserver.SparkJobValid
import org.apache.spark.SparkContext
import spark.jobserver.SparkJobValidation
import org.apache.spark.sql.SQLContext
import com.typesafe.config.Config
import spark.jobserver.SparkJob
import sprouts.spark.utils.ReadMySQL
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import java.util.Calendar
import sprouts.spark.utils.WriteMongoDB
import sprouts.spark.utils.ReadMongoDB
import scala.util.Sorting

case class ItemVectorByItemProfile(label: Double, features: SparseVector)
case class SalesByItemProfile(month: Int, year: Int, item_profile_sales:Array[ItemProfileSale])
case class ItemProfileSale(itemProfileId:Int, sales:Int, categories:Array[String])

object SalesPredictionsByItemProfile extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

   val df =
      ReadMySQL.read("""
        (SELECT ordereditem.quantity,
               MONTH(order_.date) as month, YEAR(order_.date) as year, item_id
        FROM ordereditem
        INNER JOIN order_ ON ordereditem.order_id = order_.id
        INNER JOIN item ON ordereditem.item_id = item.id
        WHERE order_.date >= DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL 48 MONTH)
        AND order_.date < DATE_FORMAT(NOW() ,'%Y-%m-01')) AS data
      """,sqlContext)

   val itemProfiles = ReadMongoDB.read(sqlContext, "item_profile_item_id_map")
   
   val mapItemProfileIdCategories = sc.broadcast(itemProfiles.map { x => (x.getInt(8), x.getAs[Seq[String]](1)) }.collectAsMap.toMap)
   
   val df_itemProfiles = df.join(itemProfiles,"item_id")
    .map { x => ((x.getLong(2), x.getLong(3), x.getInt(11)), x.getInt(1)) } // Map ( (month, year, itemProfileId), sales). (month, year) as key
    .reduceByKey(_ + _) // We obtain the sales for each month
    .map {
        x => // Map each ((month,year),sales) with a vector, with consists of (label=sales, features=(month,year))
          // SparseVector: 3 = number of features, (0, 1, 2) = indexes
          ItemVectorByItemProfile(x._2.doubleValue(), new SparseVector(3, Array(0, 1, 2), Array(x._1._1.doubleValue(), x._1._2.doubleValue(), x._1._3.doubleValue())))
    }
        
    // Let's create a dataframe of label and features
    val data = sqlContext.createDataFrame(df_itemProfiles).na.drop()

    // Get the model
    val model = getModel(data)
    // Get the dataframe with ItemVectors representing next 12 months
    val toPredict = sqlContext.createDataFrame(
      sc
        .parallelize(getDates(itemProfiles.rdd.map{x => x.getInt(8)}.distinct().collect()) // pass the item_profile id to generate the vectors to predict
        .map {
          x =>
            ItemVectorByItemProfile(0.0, new SparseVector(3, Array(0, 1, 2), Array(x._1.toDouble, x._2.toDouble,x._3.toDouble)))
        }))

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    val pred = model.transform(toPredict)
      .select("features", "prediction")
    val salesPred = sqlContext.createDataFrame(
      pred.rdd.map { x => ((x.getAs[SparseVector]("features").toArray(0).intValue,x.getAs[SparseVector]("features").toArray(1).intValue), // key: (month, year)
                              (x.getAs[SparseVector]("features").toArray(2).intValue, x.getDouble(1).round.toInt) //value: (item_profile, prediction)
                             ) }
        .aggregateByKey(List[(Int, Int)]())(_ ++ List(_), _ ++ _)
        .map{ x => (x._1, {
          val sorted = Sorting.stableSort(x._2, (x:(Int, Int), y:(Int, Int)) => x._2 < y._2).reverse
          sorted.take(20).map { x => ItemProfileSale(x._1, x._2, mapItemProfileIdCategories.value.get(x._1).get.toArray) }
        })}
        .map { x => 
          SalesByItemProfile(x._1._1, x._1._2, x._2)
        }
    )
      
    // We turn the predictions to case class objects
    /*val salesPred = sqlContext.createDataFrame(
         pred.rdd.map { x => ((x.getAs[SparseVector]("features").toArray(0).intValue,x.getAs[SparseVector]("features").toArray(1).intValue), // key: (month, year)
                              (x.getAs[SparseVector]("features").toArray(2).intValue, x.getDouble(1).round.toInt) //value: (item_profile, prediction)
                             )
        }.reduceByKey ((u, v) => {
          val values = List(u, v).sorted(Ordering[(Int, Int)].reverse)
          val sorted = Sorting.stableSort(values, (x:(Int, Int), y:(Int, Int)) => x._2 < y._2).reverse
          sorted.take(3)
        })
       )*/
      
      
      
      /*pred.rdd.map {
        x =>
          SaleByItemProfile(x.getAs[SparseVector]("features").toArray(0).intValue, // Gets month
            x.getAs[SparseVector]("features").toArray(1).intValue, // Gets year
            x.getAs[SparseVector]("features").toArray(2).intValue, // Get profile item id
            x.getDouble(1).round.toInt) // Gets prediction
      })*/

    WriteMongoDB.deleteAndPersistDF(salesPred, sqlContext, "sales_predictions_by_item_profiles")
  }

  // Returns the model
  // I've to research about this algorithm in order to properly configure the params
  def getModel(data: DataFrame): TrainValidationSplitModel = {
    val lr = new LinearRegression()
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
    // 80% of the data will be used for training and the remaining 20% for validation.
    // .setTrainRatio(0.8)

    // Run train validation split, and choose the best set of parameters.
    trainValidationSplit.fit(data)
  }

  def getDates(itemProfiles : Array[Int]): Array[(Int, Int, Int)] = {
    val date = Calendar.getInstance()
    date.add(Calendar.MONTH, -1)
    val months = 1.to(12).toArray
    val res = for {i <- months} yield { date.add(Calendar.MONTH, 1); (date.get(Calendar.MONTH) + 1, date.get(Calendar.YEAR)) }
    for {i <- res; ip <-itemProfiles} yield { (i._1,i._2, ip) }
  }

}
