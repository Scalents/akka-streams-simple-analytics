package scalents.dataservices

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer

import scala.concurrent.duration._

object DataServices extends App{

  implicit val aSys: ActorSystem = ActorSystem("data-services")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val shutdownDeadline: FiniteDuration = 10.seconds

  val routes = path( "marketprice" / Segment) { productId =>
    get {
      complete(Data.marketPrice(productId))
    }
  } ~ extractRequest { req =>
    println(s" Request $req")
    complete(StatusCodes.NotFound)
  }

  Http().bindAndHandle(routes, "localhost", 8080)
}

object Data {


  val marketPrice =  Map(
    "1" -> "99456",
    "2" -> "230",
    "3" -> "5"
  )

}
