package scalents.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}
import scalents.streams.Data._

import scala.collection.immutable
import scala.concurrent.duration._



object OrderGraph extends App {

  implicit val system = ActorSystem("troupe")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  def orderCountByCustomer(orders: Seq[FullfillOrder]) = orders.foldLeft(Map[String, Int]())((map, order) => {
    val current = map.getOrElse(order.customer.id, 0)
    map + (order.customer.id -> (current +1))
  })

  def productCountByCustomer(orders: Seq[FullfillOrder]) = orders.foldLeft(Map[String, Int]())((map, order) => {
    val current = map.getOrElse(order.customer.id, 0)
    map + (order.customer.id -> (current +order.quantity))
  })

  def productTotalByCustomer(orders: Seq[FullfillOrder]) = orders.foldLeft(Map[String, BigDecimal]())((map, order) => {
    val current = map.getOrElse(order.customer.id, BigDecimal(0))
    map + (order.customer.id -> (current + order.orderTotal))
  })

  def total(orders: Seq[FullfillOrder]) = orders.foldLeft(BigDecimal(0))((total, order) => total + order.orderTotal)

  def avg(orders: Seq[FullfillOrder]) = {
    val (total, count) = orders.foldLeft((0,BigDecimal(0))) { case ( (accCount, accTotal), order) => (accCount +1 , accTotal + order.orderTotal)}
  }

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import akka.stream.scaladsl.GraphDSL.Implicits._

    val out = Sink.foreach(println)
    val getMarketPriceFlow = HttpServiceRequestFlow[(Customer, Product, Int), BigDecimal]( t => s"/marketprice/${t._2.sku}")

    val orderFlow = Source(OrdersGenerator()).throttle(1, 500.millis)
      .mapAsync(4) { t => FalsoDB.findCostumer(t._1).map { cust => t.copy(_1 = cust) } } //get customer data
      .mapAsync(4) { t => FalsoDB.findProduct(t._2).map { prod => t.copy(_2 = prod) } }//get product data
      .via(getMarketPriceFlow)
      .mapAsync(4){ case (response, (customer, product, qty)) => Unmarshal(response).to[String].map( price => FullfillOrder(product, customer, qty, BigDecimal(price))  )}

    val countByProd = Flow[FullfillOrder].groupedWithin(1000, 10.seconds).map( orders => productCountByCustomer(orders)).map(_.map { case (id, total) => s" Customer $id = $total"}.mkString )
    val totalByProd = Flow[FullfillOrder].groupedWithin(1000, 10.seconds).map( orders => productTotalByCustomer(orders)).map(_.map { case (id, total) => s" Product $id = $total"}.mkString )

    val totalAll = Flow[FullfillOrder].groupedWithin(10000, 30.seconds).map( orders => total(orders)).map( tot => s"Total = $$$tot")
    val avgOrder = Flow[FullfillOrder].groupedWithin(10000, 30.seconds).map( orders => avg(orders)).map( tot => s"Averages = $$$tot")

    //fold by total
    val bcast = builder.add(Broadcast[FullfillOrder](4))

    orderFlow ~> bcast  ~> out
    bcast ~> countByProd ~> out
    bcast ~> totalByProd ~> out
    bcast ~> totalAll ~> out
    bcast ~> avgOrder ~> out
    ClosedShape
  })

  g.run()
}

