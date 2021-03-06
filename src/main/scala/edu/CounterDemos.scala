package edu

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.{ActorMaterializer, SourceShape, Attributes}
import akka.stream.scaladsl.{Sink, ZipWith, GraphDSL, Source}
import scala.concurrent.duration._

object ZipDemo {
  val numericStream: Source[Long, NotUsed] =
    Source(Stream.iterate(0L)(_ + 1))
  val tick: Source[Unit, Cancellable] = 
    Source.tick(0 seconds, 0.4 seconds, ())

  def zippedSource[A](inputTick: Source[Unit, A]): 
  Source[Long, NotUsed] =
    Source.fromGraph(GraphDSL.create() { 
      implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val zipWith = ZipWith[Unit, Long, Long](
        (a: Unit, i: Long) => i
      )
      val zipWithSmallBuffer = zipWith.withAttributes(
        Attributes.inputBuffer(initial = 1, max = 1))
      val zipNode = builder.add(zipWithSmallBuffer)
      inputTick ~> zipNode.in0
      numericStream ~> zipNode.in1
      SourceShape(zipNode.out)
    })

  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    zippedSource(tick).runWith(Sink.foreach(println))
  }
}

object ConflateDemo {
  val fastNumeric = ZipDemo.zippedSource(
    Source.tick(0 seconds, 0.2 seconds, ()))
  val slowTick = Source.tick(0 seconds, 1 seconds, ())

  def conflateFastThroughSlow: Source[Long, NotUsed] =
    Source.fromGraph(GraphDSL.create() { 
      implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val zipWith = 
        ZipWith[Unit, Long, Long]((a: Unit, i: Long) => i)
      val zipWithSmallBuffer = 
        zipWith.withAttributes(
          Attributes.inputBuffer(initial = 1, max = 1))
      val zipNode = builder.add(zipWithSmallBuffer)
      slowTick ~> zipNode.in0
      fastNumeric.conflate(
        (acc, elem) => elem
      ) ~> zipNode.in1
      SourceShape(zipNode.out)
    })

  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    conflateFastThroughSlow.runWith(Sink.foreach(println))
  }
}

object ExpandDemo {
  val slowNumeric = ZipDemo.zippedSource(
    Source.tick(0 seconds, 1 seconds, ()))
  val fastTick = Source.tick(0 seconds, 0.2 seconds, ())

  def expandSlowThroughFast: Source[Option[Long], NotUsed] =
    Source.fromGraph(GraphDSL.create() { 
      implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val zipWith = 
        ZipWith[Unit, Option[Long], Option[Long]](
          (a: Unit, i: Option[Long]) => i)
      val zipWithSmallBuffer = 
        zipWith.withAttributes(
          Attributes.inputBuffer(initial = 1, max = 1))
      val zipNode = builder.add(zipWithSmallBuffer)
      fastTick ~> zipNode.in0
      slowNumeric.expand[Option[Long]](
        elem => Iterator(Some(elem)) ++ Iterator.continually(None)
      ) ~> zipNode.in1
      SourceShape(zipNode.out)
    })

  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    expandSlowThroughFast.runWith(
      Sink.foreach(println)
    )
  }
}

object ScanDemo {
  val numericStream: Source[Long, NotUsed] =
    Source(Stream.iterate(0L)(_ + 1))
  val tick: Source[Unit, Cancellable] = 
    Source.tick(0 seconds, 0.4 seconds, ())

  def zipScannedSource(
    inputTick: Source[Unit, Cancellable]): 
  Source[Long, NotUsed] =
    Source.fromGraph(GraphDSL.create() { 
      implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val zipWith = 
        ZipWith[Unit, Long, Long](
          (a: Unit, i: Long) => i
        )
      val zipWithSmallBuffer = 
        zipWith.withAttributes(
          Attributes.inputBuffer(initial = 1, max = 1)
        )
      val zipNode = builder.add(zipWithSmallBuffer)
      inputTick ~> zipNode.in0
      numericStream.scan(0L)(_ + _) ~> zipNode.in1
      SourceShape(zipNode.out)
    })

  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    zipScannedSource(tick).runWith(Sink.foreach(println))
  }
}

object SwitchingTickDemo {
  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    ZipDemo.zippedSource(
      SwitchingTick(0.1 seconds, 0.3 seconds, 2.5 seconds)
    ).runWith(Sink.foreach(println))
  }
}

object ExpandConflateDemo {
  val irregularCounter =
    ZipDemo.zippedSource(
      SwitchingTick(0.1 seconds, 1 seconds, 5 seconds)
    )
  val mediumTick = Source.tick(0 seconds, 0.4 seconds, ())

  def conflateFastThroughSlow: Source[Option[Long], NotUsed] =
    Source.fromGraph(GraphDSL.create() { 
      implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val zipWith = 
        ZipWith[Unit, Option[Long], Option[Long]](
          (a: Unit, i: Option[Long]) => i
        )
      val zipWithSmallBuffer = 
        zipWith.withAttributes(
          Attributes.inputBuffer(initial = 1, max = 1)
        )
      val zipNode = builder.add(zipWithSmallBuffer)
      mediumTick ~> zipNode.in0
      irregularCounter
        .conflate((acc, elem) => elem)
        .expand[Option[Long]](elem => Iterator(Some(elem)) ++ Iterator.continually(None)) ~> zipNode.in1
      SourceShape(zipNode.out)
    })

  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    conflateFastThroughSlow.runWith(Sink.foreach(println))
  }
}

object DetachedStageDemo {
  def run = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val irregularCounter = 
      ZipDemo.zippedSource(
        SwitchingTick(0.1 seconds, 1 seconds, 5 seconds)
      )

    Transformations.zipWithTick(
      Source.tick(0 seconds, 0.4 seconds, ()),
      irregularCounter.transform(
        () => new LastElemOption[Long])
      )
    .runWith(Sink.foreach(println))
  }
}
