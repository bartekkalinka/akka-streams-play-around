package edu

import akka.Done
import akka.stream.scaladsl._
import scala.concurrent.Future

object Main {
  def main(args: Array[String]): Unit = {
    println("Start")
    counterDemos()
    //ThrottleUsage.run()
  }

  def counterDemos(): Unit = {
    //ZipDemo.run
    //ConflateDemo.run
    //ExpandDemo.run
    //ScanDemo.run
    //SwitchingTickDemo.run
    ExpandConflateDemo.run
    //DetachedStageDemo.run
  }
}

object Utils {
  def myLoggingSink[A]: Sink[A, Future[Done]] = Sink.foreach(println)
}