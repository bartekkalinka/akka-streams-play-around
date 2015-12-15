package edu

import akka.stream.scaladsl._
import scala.concurrent.Future

object Main {
  def main(args: Array[String]): Unit = {
    println("Start")
    //ZipDemo.run
    //ConflateDemo.run
    //ExpandDemo.run
    //ScanDemo.run
    //SwitchingTickDemo.run
    //ExpandConflateDemo.run
    DetachedStageDemo.run
  }
}

object Utils {
  def myLoggingSink[A]: Sink[A, Future[Unit]] = Sink.foreach(println)
}