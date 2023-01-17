package potamoi.fs.refactor

import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}
import potamoi.codecs
import S3AccessStyles.given_JsonCodec_S3AccessStyle

/**
 * Remote file system backend configuration.
 */
sealed trait FsBackendConf derives JsonCodec

/**
 * S3 file system backend configuration.
 */
case class S3FsBackendConf(
    @name("endpoint") endpoint: String,
    @name("bucket") bucket: String,
    @name("access-key") accessKey: String,
    @name("secret-key") secretKey: String,
    @name("access-style") accessStyle: S3AccessStyle = S3AccessStyle.PathStyle,
    @name("enable-ssl") sslEnabled: Boolean = false,
    @name("tmp-dir") tmpDir: String = "s3-mirror")
    extends FsBackendConf:

  def resolve(rootDataDir: String): S3FsBackendConf =
    if tmpDir.startsWith(rootDataDir) then this
    else copy(tmpDir = s"$rootDataDir/${paths.rmFirstSlash(tmpDir)}")

/**
 * S3 path access style.
 */
enum S3AccessStyle(val value: String):
  case PathStyle          extends S3AccessStyle("path-style")
  case VirtualHostedStyle extends S3AccessStyle("virtual-hosted-style")

object S3AccessStyles:
  given JsonCodec[S3AccessStyle] = codecs.simpleEnumJsonCodec(S3AccessStyle.values)

/**
 * Local file system backend configuration.
 */
case class LocalFsBackendConf(dir: String = "storage") derives JsonCodec:
  def resolve(rootDataDir: String): LocalFsBackendConf =
    if dir.startsWith(rootDataDir) then this
    else copy(dir = s"$rootDataDir/${paths.rmFirstSlash(dir)}")
