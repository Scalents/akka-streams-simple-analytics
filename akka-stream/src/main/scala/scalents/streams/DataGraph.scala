package scalents.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}
import scalents.streams.Data._

import scala.concurrent.duration._



object OrderGraph extends App {

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  def orderCountByProducer(orders: Seq[FullfillOrder]) = orders.foldLeft(Map[String, Int]())((map, order) => {
    val current = map.getOrElse(order.producer.id, 0)
    map + (order.producer.id -> (current +1))
  })

  def productCountByProducer(orders: Seq[FullfillOrder]) = orders.foldLeft(Map[String, Int]())((map, order) => {
    val current = map.getOrElse(order.producer.id, 0)
    map + (order.producer.id -> (current +order.quantity))
  })

  def productTotalByProducer(orders: Seq[FullfillOrder]) = orders.foldLeft(Map[String, BigDecimal]())((map, order) => {
    val current = map.getOrElse(order.producer.id, BigDecimal(0))
    map + (order.producer.id -> (current + (order.quantity * order.unitPrice)))
  })

  def total(orders: Seq[FullfillOrder]) = orders.foldLeft(BigDecimal(0))((total, order) => total + order.quantity * order.unitPrice)


  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import akka.stream.scaladsl.GraphDSL.Implicits._

    val out = Sink.foreach(println)
    val getMarketPriceFlow = HttpServiceRequestFlow[(Customer, Product, Int), BigDecimal]( t => s"/marketprice/${t._2.sku}")

    val orderFlow = Source(OrdersGenerator()).throttle(1, 500.millis)
      .mapAsync(4) { t => FalsoDB.findCostumer(t._1).map { cust => t.copy(_1 = cust) } } //get customer data
      .mapAsync(4) { t => FalsoDB.findProduct(t._2).map { prod => t.copy(_2 = prod) } }//get product data
      .via(getMarketPriceFlow)
      .mapAsync(4){ case (response, (customer, product, qty)) => Unmarshal(response).to[String].map( price => FullfillOrder(product, customer, qty, BigDecimal(price))  )}


    val countByProd = Flow[FullfillOrder].groupedWithin(1000, 10.seconds).map( orders => productCountByProducer(orders)).map(_.map { case (id, total) => s" Customer $id = $total"}.mkString )
    val totalByProd = Flow[FullfillOrder].groupedWithin(1000, 10.seconds).map( orders => productTotalByProducer(orders)).map(_.map { case (id, total) => s" Product $id = $total"}.mkString )
    val totalAll = Flow[FullfillOrder].groupedWithin(1000, 30.seconds).map( orders => total(orders)).map( tot => s"Total = $$$tot")



    val bcast = builder.add(Broadcast[FullfillOrder](4))

    orderFlow ~> bcast  ~> out
    bcast ~> countByProd ~> out
    bcast ~> totalByProd ~> out
    bcast ~> totalAll ~> out
    ClosedShape
  })

  g.run()
}

