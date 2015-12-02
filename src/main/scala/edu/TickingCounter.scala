package edu

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.scaladsl.{ZipWith, FlowGraph, Source}
import scala.concurrent.duration._
import akka.stream.stage._

class OptionPipe[T]() extends DetachedStage[T, Option[T]] {
  private var currentValue: Option[T] = None

  override def onPush(elem: T, ctx: DetachedContext[Option[T]]): UpstreamDirective = {
    currentValue = Some(elem)
    ctx.pull()
  }

  override def onPull(ctx: DetachedContext[Option[T]]): DownstreamDirective = {
    val result = ctx.push(currentValue)
    currentValue = None
    result
  }
}

object TickingCounter {
  def slowTick: Source[Unit, Cancellable] = Source(0 seconds, 1 seconds, ())

  def zipWithTick[A](tick: Source[Unit, Cancellable], toZip: Source[A, Unit]): Source[A, Unit] = {
    Source() { implicit builder: FlowGraph.Builder[Unit] =>
      import FlowGraph.Implicits._
      val zipWith = ZipWith[Unit, A, A]((a: Unit, i: A) => i)
      val zipWithSmallBuffer = zipWith.withAttributes(Attributes.inputBuffer(initial = 1, max = 1))
      val zipNode = builder.add(zipWithSmallBuffer)
      tick ~> zipNode.in0
      toZip ~> zipNode.in1
      zipNode.out
    }
  }

  def slowCounter: Source[Long, Unit] = zipWithTick(slowTick, Source(Stream.iterate(0L)(_ + 1)))

  def sourceAccumulation[A](source: Source[A, Unit]): Source[String, Unit] =
    slowCounter.scan("")((a, b) => a + b.toString)

  def pickWithTick[A](tick: Source[Unit, Cancellable], source: Source[A, Unit]): Source[A, Unit] = {
    val takeLast: Source[A, Unit] = source.conflate(x => x)((acc, elem) => elem)
    zipWithTick(tick, takeLast)
  }

  def optionPipe[A](tick: Source[Unit, Cancellable], source: Source[A, Unit], default: A): Source[Option[A], Unit] = {
    val pipedSource = source.transform(() => new OptionPipe[A]())
    zipWithTick(tick, pipedSource)
  }

  def fasterTick: Source[Unit, Cancellable] = Source(0 seconds, 0.2 seconds, ())

  def fasterCounter: Source[Long, Unit] = zipWithTick(fasterTick, Source(Stream.iterate(0L)(_ + 1)))
  
  def fasterDropped: Source[Long, Unit] = pickWithTick(slowTick, fasterCounter)

  def slowThroughFaster: Source[Option[Long], Unit] = optionPipe(fasterTick, slowCounter, 999L)
  
  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    slowThroughFaster.runWith(Utils.myLoggingSink)
  }
}

