package edu

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.{ThrottleMode, ActorMaterializer}
import akka.stream.scaladsl.Source
import scala.concurrent.duration._

object ThrottleUsage {
  def run(): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val fastTick: Source[Unit, Cancellable] = Source.tick(0 seconds, 0.2 seconds, ())

    fastTick.throttle(1, 5 seconds, 1, ThrottleMode.shaping).runWith(Utils.myLoggingSink)
  }
}
