package scalents.streams

import scalents.streams.Data.{Customer, Product}
import spray.json.{DefaultJsonProtocol, JsonFormat}

import scala.collection.immutable.Iterable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object Data {

  case class Customer(id: String, desc: String)

  case class Product(sku: String, desc: String)

  case class FullfillOrder(product: Product, customer: Customer, quantity: Int, unitPrice: BigDecimal){
    def orderTotal: BigDecimal = quantity * unitPrice
  }

  object JsonProtocol extends DefaultJsonProtocol {
    implicit val customerFmt: JsonFormat[Customer] = jsonFormat2(Customer)
    implicit val productFmt: JsonFormat[Product] = jsonFormat2(Product)
  }

}

object FalsoDB {

  private val customers = List(
    Customer("a","House Atrevides"),
    Customer("b","House Harpoonen"),
    Customer("c","House Cordero")
  )

  private val products = List(
    Product("1", "Spice"),
    Product("2", "Sandrat meat"),
    Product("3", "Ice cubes")
  )

  def findCostumer(producerId: String)(implicit ec: ExecutionContext) = Future { customers.find(_.id == producerId).get }
  def findProduct(productId: String)(implicit ec: ExecutionContext) = Future { products.find(_.sku == productId).get }

}



case class OrdersGenerator() extends Iterable[(String, String, Int)] {

  val products = Vector("1","2", "3")
  val customers = Vector("a", "b", "c")

  private var random = new Random()
  override def iterator: Iterator[(String, String, Int)] = new Iterator[(String, String, Int)] {
    override def hasNext: Boolean = true

    override def next(): (String, String, Int) = {
      val producer = customers(random.nextInt(customers.size))
      val product = products(random.nextInt(products.size))
      val quantity = random.nextInt(1000)
      (producer, product, quantity)
    }
  }
}



