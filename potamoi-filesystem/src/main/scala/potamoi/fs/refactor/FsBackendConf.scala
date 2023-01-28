package potamoi.fs.refactor

import com.typesafe.config.Config
import potamoi.{codecs, BaseConf, HoconConfig}
import potamoi.fs.refactor.S3AccessStyles.given_JsonCodec_S3AccessStyle
import zio.{ZIO, ZLayer}
import zio.config.magnolia.{descriptor, name}
import zio.config.read
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

/**
 * Remote file system backend configuration.
 */
sealed trait FsBackendConf:
  type This <: FsBackendConf
  def resolve(rootDataDir: String): This

object FsBackendConf:

  val live: ZLayer[BaseConf with Config, Throwable, FsBackendConf] = ZLayer {
    for {
      baseConf      <- ZIO.service[BaseConf]
      source        <- HoconConfig.hoconSource("potamoi.fs-backend")
      config        <- read(descriptor[FsBackendConf].from(source))
      resolvedConfig = config.resolve(baseConf.dataDir)
    } yield resolvedConfig
  }

/**
 * S3 file system backend configuration.
 */
@name("s3")
case class S3FsBackendConf(
    @name("endpoint") endpoint: String,
    @name("bucket") bucket: String,
    @name("access-key") accessKey: String,
    @name("secret-key") secretKey: String,
    @name("access-style") accessStyle: S3AccessStyle = S3AccessStyle.PathStyle,
    @name("enable-ssl") sslEnabled: Boolean = false,
    @name("tmp-dir") tmpDir: String = "s3-mirror",
    @name("enable-mirror-cache") enableMirrorCache: Boolean = true)
    extends FsBackendConf:

  type This = S3FsBackendConf

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
@name("local")
case class LocalFsBackendConf(dir: String = "storage") extends FsBackendConf derives JsonCodec:

  type This = LocalFsBackendConf

  def resolve(rootDataDir: String): LocalFsBackendConf =
    if dir.startsWith(rootDataDir) then this
    else copy(dir = s"$rootDataDir/${paths.rmFirstSlash(dir)}")
