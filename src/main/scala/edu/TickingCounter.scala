package edu

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ZipWith, FlowGraph, Source}
import scala.concurrent.duration._

object TickingCounter {
  def counter: Source[Long, Unit] = zipWithTick(Source(Stream.iterate(0L)(_ + 1)))

  def constantTick: Source[Unit, Cancellable] = Source(0 seconds, 1 seconds, ())

  def zipWithTick[A](toZip: Source[A, Unit]): Source[A, Unit] = {
    Source() { implicit builder: FlowGraph.Builder[Unit] =>
      import FlowGraph.Implicits._
      val zip = builder.add(ZipWith[Unit, A, A]((a: Unit, i: A) => i))
      constantTick ~> zip.in0
      toZip ~> zip.in1
      zip.out
    }
  }

  def sourceAccumulation[A](source: Source[A, Unit]): Source[String, Unit] =
    counter.scan("")((a, b) => a + b.toString)

  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    sourceAccumulation(counter).runWith(Utils.myLoggingSink)
    //counter.runWith(Utils.myLoggingSink)
  }
}

