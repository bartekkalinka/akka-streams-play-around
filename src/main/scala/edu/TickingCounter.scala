package edu

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.{FanInShape, ActorMaterializer, Attributes}
import akka.stream.scaladsl.{FlexiMerge, ZipWith, FlowGraph, Source}
import scala.concurrent.duration._
import akka.stream.stage._

class LastElemOption[T]() extends DetachedStage[T, Option[T]] {
  private var currentValue: Option[T] = None

  override def onPush(elem: T, ctx: DetachedContext[Option[T]]): UpstreamDirective = {
    currentValue = Some(elem)
    ctx.pull()
  }

  override def onPull(ctx: DetachedContext[Option[T]]): DownstreamDirective = {
    val previousValue = currentValue
    currentValue = None
    ctx.push(previousValue)
  }
}

import akka.stream.FanInShape._
class SwitchPorts[A, B](_init: Init[(Unit, A, B)] = Name("Switch"))
  extends FanInShape[(Unit, A, B)](_init) {
  val trigger = newInlet[Unit]("trigger")
  val left = newInlet[A]("left")
  val right = newInlet[B]("right")
  protected override def construct(i: Init[(A, B)]) = new SwitchPorts(i)
}
class Switch[A, B] extends FlexiMerge[(A, B), SwitchPorts[A, B]](
  new SwitchPorts, Attributes.name("Switch2State")) {
  import FlexiMerge._

  override def createMergeLogic(p: PortT) = new MergeLogic[(A, B)] {
    var direction: Boolean
    var lastInA: A = _

    val readTrigger: State[Unit] = State[Unit](Read(p.trigger)) { (ctx, input, element) =>
      direction = !direction
    }

    val readA: State[A] = State[A](Read(p.left)) { (ctx, input, element) =>
      lastInA = element
      readB
    }

    val readB: State[B] = State[B](Read(p.right)) { (ctx, input, element) =>
      ctx.emit((lastInA, element))
      readA
    }

    override def initialState: State[_] = readA

    override def initialCompletionHandling = eagerClose
  }
}

object TickingCounter {
  def slowTick: Source[Unit, Cancellable] = Source(0 seconds, 3 seconds, ())
  def mediumTick: Source[Unit, Cancellable] = Source(0 seconds, 1 seconds, ())
  def fastTick: Source[Unit, Cancellable] = Source(0 seconds, 0.2 seconds, ())

  def zipWithTick[A](tick: Source[Unit, Cancellable], toZip: Source[A, Unit]): Source[A, Unit] =
    Source() { implicit builder: FlowGraph.Builder[Unit] =>
      import FlowGraph.Implicits._
      val zipWith = ZipWith[Unit, A, A]((a: Unit, i: A) => i)
      val zipWithSmallBuffer = zipWith.withAttributes(Attributes.inputBuffer(initial = 1, max = 1))
      val zipNode = builder.add(zipWithSmallBuffer)
      tick ~> zipNode.in0
      toZip ~> zipNode.in1
      zipNode.out
    }

  def counter(tick: Source[Unit, Cancellable]): Source[Long, Unit] = zipWithTick(tick, Source(Stream.iterate(0L)(_ + 1)))

  def sourceAccumulation[A](source: Source[A, Unit]): Source[String, Unit] =
    counter(mediumTick).scan("")((a, b) => a + b.toString)

  def conflateToLast[A](tick: Source[Unit, Cancellable], source: Source[A, Unit]): Source[A, Unit] = {
    val takeLast: Source[A, Unit] = source.conflate(x => x)((acc, elem) => elem)
    zipWithTick(tick, takeLast)
  }

  def lastElemOption[A](tick: Source[Unit, Cancellable], source: Source[A, Unit]): Source[Option[A], Unit] = {
    val pipedSource = source.transform(() => new LastElemOption[A]())
    zipWithTick(tick, pipedSource)
  }
  
  def mediumDropped: Source[Long, Unit] = conflateToLast(mediumTick, counter(fastTick))

  def mediumThroughFast: Source[Option[Long], Unit] = lastElemOption(fastTick, counter(mediumTick))
  
  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    mediumThroughFast.runWith(Utils.myLoggingSink)
  }
}

