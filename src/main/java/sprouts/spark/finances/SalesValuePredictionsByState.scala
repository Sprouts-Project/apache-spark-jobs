package sprouts.spark.finances

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

case class ItemVectorValueByState(label: Double, features: SparseVector)
case class SaleVByState(month: Int, year: Int, state: String, salesValue: Double)

object SalesValuePredictionsByState extends SparkJob {
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
(SELECT order_.totalPrice,
       MONTH(order_.date) as month, YEAR(order_.date) as year, customer.state
FROM order_
INNER JOIN customer ON order_.customer_id = customer.id
AND order_.date >= DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL 48 MONTH)
 AND order_.date < DATE_FORMAT(NOW() ,'%Y-%m-01')) AS data
      """

    val main_df = ReadMySQL.read(mySQLquery, sqlContext).rdd
      .map { x => ((x.getLong(1), x.getLong(2), x.getString(3)), x.getDouble(0)) } // Map ( (month, year, state), salesValue). (month, year, state) as key

    val statesIds = main_df.map { x => x._1 }.map { x => (x._3) }.distinct().zipWithIndex()
    val mapStateIdToIndex = sc.broadcast(statesIds.collectAsMap.toMap) // State mapped to index
    val mapIndexToStateId = sc.broadcast(statesIds.map(_.swap).collectAsMap().toMap) // index mapped to State

    val df = main_df.reduceByKey(_ + _) // We obtain the sales for each month
      .map {
        x => // Map each ((month,year,state),sales) with a vector, with consists of (label=sales, features=(month,year))
          // SparseVector: 3 = number of features, (0, 1, 2) = indexes
          ItemVectorValueByState(x._2.doubleValue(), new SparseVector(3, Array(0, 1, 2), Array(x._1._1.doubleValue(), x._1._2.doubleValue(), mapStateIdToIndex.value.get(x._1._3).get.doubleValue())))
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
              ItemVectorValueByState(0.0, new SparseVector(3, Array(0, 1, 2), Array(x._1.toDouble, x._2.toDouble, x._3.toDouble)))
          }))

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    val pred = model.transform(toPredict)
      .select("features", "prediction")

    // We turn the predictions to case class objects
    val salesPred = sqlContext.createDataFrame(
      pred.rdd.map {
        x =>
          SaleVByState(x.getAs[SparseVector]("features").toArray(0).intValue, // Gets month
            x.getAs[SparseVector]("features").toArray(1).intValue, // Gets year
            mapIndexToStateId.value.get(x.getAs[SparseVector]("features").toArray(2).longValue()).get, // Gets state
            BigDecimal(x.getDouble(1)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) // Gets prediction and truncated to 2 decimals
      })

    WriteMongoDB.deleteAndPersistDF(salesPred, sqlContext, "sales_value_predictions_by_state")
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
