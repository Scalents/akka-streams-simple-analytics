package scalents.streams

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.unmarshalling.Unmarshaller._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.scaladsl.{FileIO, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import scalents.streams.Data._

object OrderFlow extends App{


  /*
  Streams! Streams offer an interesting conceptual model to processing pipelines that is very functional programming oriented.
  The streaming paradigm is very well suited to deal with a constant flow of data and Akka streams is a powerful implementation of it.
  It offers a set of composable building blocks for creating asynchronous and scalable data streaming applications.
  In this talk we will live code from a very basic stream to a data aggregation pipeline that interacts with multiple services
   */

  //Source
  // producer id, product sku, quantity
  // get product data from service
  // create shipment every x products
  // get top customers
  // get bottom customers
  // get market price
  // calculate total earnings

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec = system.dispatcher


  val source = Source(OrdersGenerator())

  source.take(1000)
    .mapAsync(4) { t => FalsoDB.findCostumer(t._1).map { cust => t.copy(_1 = cust) } } //get customer data
    .mapAsync(4) { t => FalsoDB.findProduct(t._2).map { prod => t.copy(_2 = prod) } }//get product data
    .via(getMarketPriceFlow)
    .mapAsync(4){ case (response, (customer, product, qty)) => Unmarshal(response).to[String].map( price => FullfillOrder(product, customer, qty, BigDecimal(price))  )}
    //send order to service
    //retry
    //.runForeach(println)
    .map(order => ByteString(s"$order\n"))
    .runWith(FileIO.toPath(Paths.get("orders.txt")))
    .onComplete(_ ⇒ system.terminate())



  def getMarketPriceFlow = HttpServiceRequestFlow[(Customer, Product, Int), BigDecimal]( t => s"/marketprice/${t._2.sku}")
}

