package edu

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.{SourceShape, FanInShape2, ActorMaterializer, Attributes}
import akka.stream.scaladsl.{Broadcast, ZipWith, GraphDSL, Source, Merge}
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

object SwitchingTick {
  import Transformations._

  def apply(firstInterval: FiniteDuration, secondInterval: FiniteDuration, switchingInterval: FiniteDuration): Source[Unit, Unit] = {
    assert(firstInterval < switchingInterval && secondInterval < switchingInterval, "Switching interval should be longer than switched intervals")
    val switchingTick = Source.tick(0 seconds, switchingInterval, ())
    val firstTick = Source.tick(0 seconds, firstInterval, ())
    val secondTick = Source.tick(0 seconds, secondInterval, ())
    Source.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[Unit] =>
      import GraphDSL.Implicits._
      val broadcastNode = builder.add(Broadcast[Unit](2))
      switchingTick ~> broadcastNode.in
      val neg: ((Boolean, Unit) => Boolean) = {case (a, ()) => !a}
      val zipNode1 = zipWithNode[Boolean]
      firstTick ~> zipNode1.in0
      broadcastNode.out(0).scan(false)(neg).expand(identity)(a => (a, a)).outlet ~> zipNode1.in1
      val zipNode2 = zipWithNode[Boolean]
      secondTick ~> zipNode2.in0
      broadcastNode.out(1).scan(true)(neg).expand(identity)(a => (a, a)).outlet ~> zipNode2.in1
      val merge = builder.add(Merge[Boolean](2))
      zipNode1.out ~> merge.in(0)
      zipNode2.out ~> merge.in(1)
      SourceShape(merge.out.filter(identity).map(a => ()).outlet)
    })
  }
}

object Ticks {
  def slowTick: Source[Unit, Cancellable] = Source.tick(0 seconds, 3 seconds, ())
  def mediumTick: Source[Unit, Cancellable] = Source.tick(0 seconds, 1 seconds, ())
  def fastTick: Source[Unit, Cancellable] = Source.tick(0 seconds, 0.2 seconds, ())
  def switchingTick: Source[Unit, Unit] = SwitchingTick(0.05 seconds, 0.7 seconds, 3 seconds)
}

object Counter {
  def apply[A](tick: Source[Unit, A]): Source[Long, Unit] = Transformations.zipWithTick(tick, Source(Stream.iterate(0L)(_ + 1)))
}

object Transformations {
  def zipWithNode[A](implicit builder: GraphDSL.Builder[Unit]): FanInShape2[Unit, A, A] =  {
    val zipWith = ZipWith[Unit, A, A]((a: Unit, i: A) => i)
    val zipWithSmallBuffer = zipWith.withAttributes(Attributes.inputBuffer(initial = 1, max = 1))
    builder.add(zipWithSmallBuffer)
  }

  def zipWithTick[A, B](tick: Source[Unit, B], toZip: Source[A, Unit]): Source[A, Unit] =
    Source.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[Unit] =>
      import GraphDSL.Implicits._
      val zipNode = zipWithNode[A]
      tick ~> zipNode.in0
      toZip ~> zipNode.in1
      SourceShape(zipNode.out)
    })

  def sourceAccumulation[A](source: Source[A, Unit]): Source[String, Unit] =
    source.scan("")((a, b) => a + b.toString)

  def conflateToLast[A](tick: Source[Unit, Cancellable], source: Source[A, Unit]): Source[A, Unit] = {
    val takeLast: Source[A, Unit] = source.conflate(x => x)((acc, elem) => elem)
    zipWithTick(tick, takeLast)
  }

  def lastElemOption[A](tick: Source[Unit, Cancellable], source: Source[A, Unit]): Source[Option[A], Unit] = {
    val pipedSource = source.transform(() => new LastElemOption[A]())
    zipWithTick(tick, pipedSource)
  }
}

object CountersDemo {
  import Ticks._
  import Transformations._

  def mediumDropped: Source[Long, Unit] = conflateToLast(mediumTick, Counter(fastTick))

  def mediumThroughFast: Source[Option[Long], Unit] = lastElemOption(fastTick, Counter(mediumTick))

  def switchingThroughFast: Source[Option[Long], Unit] = lastElemOption(fastTick, Counter(switchingTick))

  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    switchingThroughFast.runWith(Utils.myLoggingSink)
  }
}

