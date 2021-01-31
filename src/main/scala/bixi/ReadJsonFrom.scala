package bixi

import java.net.URL
import scala.io._
import cats.implicits._
import io.circe._
import io.circe.parser._

object ReadJsonFrom {
  def urlInto[A: Decoder](url: String): Either[Throwable, A] =
    getUrl(url).flatMap(urlInto[A])

  private[this] def getUrl(url: String): Either[Throwable, URL] =
  Either.catchNonFatal(new URL(url))

  def urlInto[A: Decoder](url: URL): Either[Throwable, A] =
    fromURL(url)
      .map(_.getLines.mkString)
      .flatMap(decode[A])

  private[this] def fromURL(url: URL): Either[Throwable, BufferedSource] =
    Either.catchNonFatal(Source.fromURL(url))

  def resourceInto[A: Decoder](resourceName: String): Either[Throwable, A] =
    getResource(resourceName).flatMap(urlInto[A])

  private[this] def getResource(resourceName: String): Either[Throwable, URL] =
    Either.catchNonFatal(getClass.getClassLoader.getResource(resourceName))

}
