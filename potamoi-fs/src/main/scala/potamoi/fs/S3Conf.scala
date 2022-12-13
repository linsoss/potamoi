package potamoi.fs

import potamoi.common.Syntax.contra
import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}
import S3AccessStyle.*

/**
 * S3 storage configuration.
 */
case class S3Conf(
    @name("endpoint") endpoint: String,
    @name("bucket") bucket: String,
    @name("access-key") accessKey: String,
    @name("secret-key") secretKey: String,
    @name("access-style") accessStyle: S3AccessStyle = S3AccessStyle.PathStyle,
    @name("enable-ssl") sslEnabled: Boolean = false):
  /**
   * Modify s3 path to the correct access style.
   */
  def revisePath(s3Path: String): String = {
    val segs = s3Path.split("://")
    if (segs.length < 2) s3Path
    else
      val purePathSegs = segs(1).split('/')
      val revisePathSegs = accessStyle match
        case PathStyle          => if (purePathSegs.head == bucket) purePathSegs else Array(bucket) ++ purePathSegs
        case VirtualHostedStyle => if (purePathSegs.head == bucket) purePathSegs.drop(1) else purePathSegs
      segs(0) + "://" + revisePathSegs.mkString("/")
  }

object S3Conf:
  import S3AccessStyles.given
  given JsonCodec[S3Conf] = DeriveJsonCodec.gen[S3Conf]

/**
 * S3 path access style.
 */
enum S3AccessStyle(val value: String):
  case PathStyle          extends S3AccessStyle("path-style")
  case VirtualHostedStyle extends S3AccessStyle("virtual-hosted-style")

object S3AccessStyles:
  given JsonCodec[S3AccessStyle] = JsonCodec(
    JsonEncoder[String].contramap(_.value),
    JsonDecoder[String].map(s => S3AccessStyle.values.find(_.value == s).getOrElse(S3AccessStyle.PathStyle))
  )
