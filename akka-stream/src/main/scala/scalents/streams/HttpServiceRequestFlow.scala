package scalents.streams

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object HttpServiceRequestFlow {

  val hostname = "localhost"
  val hostport = 8080

  private def requestFlow[IN](fResourceUri: IN => String) = Flow[IN].map( in => (HttpRequest(method = HttpMethods.GET, uri = fResourceUri(in)), in))

  private def responseFlow[IN, OUT](implicit ec: ExecutionContext, um: Unmarshaller[HttpResponse, OUT], mat: Materializer) = Flow[(Try[HttpResponse], IN)].mapAsync(4) {
    case (response, in) =>
      for {
        res <- Future.fromTry(response)
        data <- Unmarshal(res).to[OUT]
      } yield (data, in)
  }

  private def connectionFlow[IN](implicit as: ActorSystem)= Http().cachedHostConnectionPool[IN](hostname, hostport)

  def apply[IN, OUT](fResourceUri: IN => String)(implicit ec: ExecutionContext, as: ActorSystem, mat: Materializer) = Flow[IN].via(requestFlow(fResourceUri)).via(connectionFlow).via(responseFlow)

}
