package sprouts.spark.stock

import org.apache.spark.SparkContext
import spark.jobserver.SparkJob

import com.typesafe.config.Config
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobValid
import org.apache.spark.sql.SQLContext
import sprouts.spark.utils.ReadMySQL
import sprouts.spark.utils.WriteMongoDB

case class Sales(month: Int, year: Int, numProducts: Int)
case class SaleStockByState(month: Int, year: Int, numProducts: Double, state: String)
case class TopSaleProducts(id: Int, title: String, quantity: Int)

case class StockOverview(monthly_sales: Array[Sales], monthly_sales_by_state: Array[SaleStockByState], top_products: Array[TopSaleProducts])

object StockOverview extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    execute(sc)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid // Always valid
  }

  def execute(sc: SparkContext): Any = {
    val sqlContext = SQLContext.getOrCreate(sc)

    val stocks = ReadMySQL.read("(SELECT item_id, title, quantity, YEAR(date) as year, MONTH(date) as month,state FROM order_ " +
      " JOIN customer on customer.id = order_.customer_id JOIN ordereditem on order_.id = ordereditem.order_id JOIN item on item.id = ordereditem.item_id " +
      " WHERE date > DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL 24 MONTH)) AS data", sqlContext)

    // The total monthly sales value during the last 24 months
    val salesStocks = stocks.map { x => ((x.getLong(3), x.getLong(4)), x.getInt(2)) }
      .reduceByKey(_ + _)
      .map {
        x => Sales(x._1._2.intValue(), x._1._1.intValue(), x._2);
      }.collect()

    // The total monthly sales value during the last 24 months grouped by state
    val salesStocksByState = stocks.map { x => ((x.getLong(3), x.getLong(4), x.getString(5)), x.getInt(2)) }
      .reduceByKey(_ + _)
      .map {
        x => SaleStockByState(x._1._2.intValue(), x._1._1.intValue(), x._2, x._1._3);
      }.collect()

    // Top 20 most monthly demanded products during the last 24 months.
    val topSaleProducts = stocks.map { x => ((x.getInt(0), x.getString(1)), x.getInt(2)) }
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(20)
      .map {
        x => TopSaleProducts(x._1._1, x._1._2, x._2);
      }

    // Save and response
    val stockOverview =
      sqlContext.createDataFrame(List(StockOverview(salesStocks, salesStocksByState, topSaleProducts)))
    WriteMongoDB.deleteAndPersistDF(stockOverview, sqlContext, "stock_overview");
  }
}
