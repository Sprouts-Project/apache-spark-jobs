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

case class ItemVectorByState(label: Double, features: SparseVector)
case class SalesByState(month: Int, year: Int, statesSales: List[StateSales])
case class StateSales(state: String, sales: Int, abbreviation:String)

object SalesPredictionsByState extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    val mySQLquery =
      """
(SELECT ordereditem.quantity,
       MONTH(order_.date) as month, YEAR(order_.date) as year, state
FROM ordereditem
INNER JOIN order_ ON ordereditem.order_id = order_.id
INNER JOIN customer ON order_.customer_id = customer.id
WHERE order_.date >= DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL 48 MONTH)
AND order_.date < DATE_FORMAT(NOW() ,'%Y-%m-01')) AS data
      """

    val main_df = ReadMySQL.read(mySQLquery, sqlContext).rdd
      .map { x => ((x.getLong(1), x.getLong(2), x.getString(3)), x.getInt(0)) } // Map ( (month, year, state), sales). (month, year, state) as key

    val statesIds = main_df.map { x => x._1 }.map { x => (x._3) }.distinct().zipWithIndex()
    val mapStateIdToIndex = sc.broadcast(statesIds.collectAsMap.toMap) // State mapped to index
    val mapIndexToStateId = sc.broadcast(statesIds.map(_.swap).collectAsMap().toMap) // index mapped to State

    val mapStatesNames = sc.broadcast(ReadMongoDB.read(sqlContext, "map_state_name_abbreviation")
      .select("name", "abbreviation")
      .map { x => (x.getString(0), x.getString(1)) } // maps (state_name, abbreviation)
      .collectAsMap().toMap // convert it to a map
    )
    
    val df = main_df.reduceByKey(_ + _) // We obtain the sales for each month
      .map {
        x => // Map each ((month,year, state),sales) with a vector, with consists of (label=sales, features=(month,year, state_index (mapped)))
          // SparseVector: 2 = number of features, (0, 1) = indexes
          ItemVectorByState(x._2.doubleValue(), new SparseVector(3, Array(0, 1, 2), Array(x._1._1.doubleValue(), x._1._2.doubleValue(), mapStateIdToIndex.value.get(x._1._3).get.doubleValue())))
      }

    // Let's create a dataframe of label and features
    val data = sqlContext.createDataFrame(df).na.drop()

    // Get the model
    val model = getModel(data)

    // Get the dataframe with ItemVectors representing next 12 months
    val toPredict = sqlContext.createDataFrame(
      sc
        .parallelize(getDates(statesIds.map { x => x._2 }.collect())
          .map {
            x =>
              ItemVectorByState(0.0, new SparseVector(3, Array(0, 1, 2), Array(x._1.toDouble, x._2.toDouble, x._3.toDouble)))
          }))

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    val pred = model.transform(toPredict)
      .select("features", "prediction")

    // We turn the predictions to case class objects
    val salesPred = sqlContext.createDataFrame(
      pred.rdd.map {
        x =>
          ((x.getAs[SparseVector]("features").toArray(0).intValue, x.getAs[SparseVector]("features").toArray(1).intValue), // key: (month, year)
            (StateSales(
                  mapIndexToStateId.value.get(x.getAs[SparseVector]("features").toArray(2).longValue()).get, // gets state
                  x.getDouble(1).round.toInt, // Gets prediction
                  "US-"+mapStatesNames.value.get(mapIndexToStateId.value.get(x.getAs[SparseVector]("features").toArray(2).longValue()).get).get // sets the state abbreviation
            ))) // value: StateSales(state, prediction, abb) 
        }
        .aggregateByKey(List[StateSales]())(_ ++ List(_), _ ++ _)
        .map{ x => SalesByState(
                x._1._1,
                x._1._2,
                x._2
        )}
      )
      

    WriteMongoDB.deleteAndPersistDF(salesPred, sqlContext, "sales_predictions_by_state")
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

    // Run train validation split, and choose the best set of parameters.
    trainValidationSplit.fit(data)
  }

  def getDates(states: Array[Long]): Array[(Int, Int, Int)] = {
    val date = Calendar.getInstance()
    date.add(Calendar.MONTH, -1)
    val months = 1.to(12).toArray
    val res = for { i <- months } yield { date.add(Calendar.MONTH, 1); (date.get(Calendar.MONTH) + 1, date.get(Calendar.YEAR)) }
    for { i <- res; s <- states } yield { (i._1, i._2, s.intValue) }
  }

}
