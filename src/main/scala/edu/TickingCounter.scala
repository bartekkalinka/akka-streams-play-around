package edu

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.{ActorMaterializerSettings, ActorMaterializer}
import akka.stream.scaladsl.{Flow, ZipWith, FlowGraph, Source}
import scala.concurrent.duration._

object TickingCounter {
  def slowTick: Source[Unit, Cancellable] = Source(0 seconds, 1 seconds, ())

  def zipWithTick[A](tick: Source[Unit, Cancellable], toZip: Source[A, Unit]): Source[A, Unit] = {
    Source() { implicit builder: FlowGraph.Builder[Unit] =>
      import FlowGraph.Implicits._
      val zip = builder.add(ZipWith[Unit, A, A]((a: Unit, i: A) => i))
      tick ~> zip.in0
      toZip ~> zip.in1
      zip.out
    }
  }

  def counter: Source[Long, Unit] = zipWithTick(slowTick, Source(Stream.iterate(0L)(_ + 1)))

  def sourceAccumulation[A](source: Source[A, Unit]): Source[String, Unit] =
    counter.scan("")((a, b) => a + b.toString)

  def pickWithTick[A](tick: Source[Unit, Cancellable], source: Source[A, Unit]): Source[A, Unit] = {
    val takeLast: Source[A, Unit] = source.conflate(x => x)((acc, elem) => elem)
    zipWithTick(tick, takeLast)
  }

  def fasterCounter: Source[Long, Unit] = zipWithTick(Source(0 seconds, 0.2 seconds, ()), Source(Stream.iterate(0L)(_ + 1)))
  
  def fasterDropped: Source[Long, Unit] = pickWithTick(slowTick, fasterCounter)
  
  def run = {
    implicit val system = ActorSystem()
    val settings = ActorMaterializerSettings(system).withInputBuffer(1, 1)
    implicit val materializer = ActorMaterializer(settings)

    fasterDropped.runWith(Utils.myLoggingSink)
    //sourceAccumulation(counter).runWith(Utils.myLoggingSink)
    //counter.runWith(Utils.myLoggingSink)
  }
}

