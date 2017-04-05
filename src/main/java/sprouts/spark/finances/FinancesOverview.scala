package sprouts.spark.finances

import org.apache.spark.SparkContext
import spark.jobserver.SparkJob

import com.typesafe.config.Config
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobValid
import org.apache.spark.sql.SQLContext
import sprouts.spark.utils.ReadMySQL
import sprouts.spark.utils.WriteMongoDB

case class SaleValue(month: Int, year: Int, SalesValue: Double)
case class SaleValueByState(month: Int, year: Int, salesValue: Double, state: String)
case class FinacesOverview(monthly_sales: Array[SaleValue], monthly_sales_by_state: Array[SaleValueByState])

object FinancesOverview extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    val orders = ReadMySQL.read("(SELECT totalPrice, YEAR(date) as year, MONTH(date) as month,state FROM order_ join customer on customer.id = order_.customer_id WHERE date > DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL 24 MONTH)) AS data", sqlContext)

    // The total monthly sales value during the last 24 months
    val salesValues = orders.map { x => ((x.getLong(1), x.getLong(2)), x.getDouble(0)) }
      .reduceByKey(_ + _)
      .map {
        x => SaleValue(x._1._2.intValue(), x._1._1.intValue(), x._2);
      }.collect()

    // The total monthly sales value during the last 24 months grouped by state
    val salesValuesByState = orders.map { x => ((x.getLong(1), x.getLong(2), x.getString(3)), x.getDouble(0)) }
      .reduceByKey(_ + _)
      .map {
        x => SaleValueByState(x._1._2.intValue(), x._1._1.intValue(), x._2, x._1._3);
      }.collect()

    // Save and response
    val finaceOverview =
      sqlContext.createDataFrame(List(FinacesOverview(salesValues, salesValuesByState)))
    WriteMongoDB.deleteAndPersistDF(finaceOverview, sqlContext, "finance_overview");
  }
}
