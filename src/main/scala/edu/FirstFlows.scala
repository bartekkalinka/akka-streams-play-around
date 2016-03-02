package edu

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

object FirstFlows {
  final case class Author(handle: String)

  final case class Hashtag(name: String)

  final case class Tweet(author: Author, timestamp: Long, body: String) {
    def hashtags: Set[Hashtag] =
      body.split(" ").collect { case t if t.startsWith("#") => Hashtag(t) }.toSet
  }

  def run = {
    val akka = Hashtag("#akka")
    implicit val system = ActorSystem("reactive-tweets")
    implicit val materializer = ActorMaterializer()

    val tweets: Source[Tweet, NotUsed] = Source(List("aa #fsdf #pojj", "kjlksd piu #kjk", "fsdkfj #akka #ppp").map(Tweet(Author("bartek"), 234234234, _)))

    val authors: Source[Author, NotUsed] =
      tweets
        .filter(_.hashtags.contains(akka))
        .map(_.author)

    val hashtags: Source[Hashtag, NotUsed] = tweets.mapConcat(_.hashtags.toList)

    hashtags.runWith(Utils.myLoggingSink)
  }
}