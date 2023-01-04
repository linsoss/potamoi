package potamoi.fs.refactor

import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

/**
 * Remote file system backend configuration.
 */
sealed trait FsBackendConf

object FsBackendConf:
  import S3AccessStyles.given
  given JsonCodec[FsBackendConf] = DeriveJsonCodec.gen[FsBackendConf]

/**
 * S3 file system backend configuration.
 */
case class S3FsBackendConf(
    @name("endpoint") endpoint: String,
    @name("bucket") bucket: String,
    @name("access-key") accessKey: String,
    @name("secret-key") secretKey: String,
    @name("access-style") accessStyle: S3AccessStyle = S3AccessStyle.PathStyle,
    @name("enable-ssl") sslEnabled: Boolean = false)
    extends FsBackendConf

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
