package potamoi.akka

import com.typesafe.config.{Config, ConfigFactory}
import potamoi.akka.AkkaConf.{when, wrapQuote}
import potamoi.HoconConfig
import potamoi.codecs.scalaDurationJsonCodec
import zio.{UIO, ULayer, ZIO, ZLayer}
import zio.config.{read, ReadError}
import zio.config.magnolia.{descriptor, name}
import zio.json.JsonCodec

import scala.concurrent.duration.{Duration, DurationInt}
import scala.jdk.CollectionConverters.*

/**
 * Akka system configuration.
 */
case class AkkaConf(
    @name("system-name") systemName: String = "potamoi",
    @name("artery-host") arteryHost: Option[String] = None,
    @name("artery-port") arteryPort: Int = 3300,
    @name("seed-addresses") seedAddresses: List[String] = List.empty,
    @name("node-roles") nodeRoles: List[String] = List.empty,
    @name("log-generated-config") logGeneratedConfig: Boolean = false,
    @name("default-spawn-timeout") defaultSpawnTimeout: Duration = 30.seconds,
    @name("default-ask-timeout") defaultAskTimeout: Duration = 1.minutes,
    @name("default-ddata") defaultDDataConf: DDataConf = DDataConf())
    derives JsonCodec:

  /**
   * Convert to raw akka hocon configuration.
   */
  def toRawAkkaConfig: UIO[String] = ZIO.succeed {
    s"""akka {
       |  actor {
       |    provider = "cluster"
       |    serializers {
       |      kryo = "io.altoo.akka.serialization.kryo.KryoSerializer"
       |    }
       |    serialization-bindings {
       |      "potamoi.KryoSerializable" = kryo
       |    }
       |  }
       |  remote.artery {
       |    ${when(arteryHost.nonEmpty)(s"canonical.hostname = ${wrapQuote(arteryHost.get)}")}
       |    canonical.port = ${arteryPort}
       |  }
       |  cluster {
       |    ${when(nodeRoles.nonEmpty)(s"roles = [${nodeRoles.toSet.map(wrapQuote).mkString(",")}]")}
       |    ${when(seedAddresses.nonEmpty)(s"seed-nodes = [${seedAddresses.toSet.map(e => wrapQuote(s"akka://$systemName@$e")).mkString(",")}]")}
       |    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
       |  }
       |  coordinated-shutdown.exit-jvm = on
       |  log-dead-letters = 3
       |}
       |""".stripMargin
      .split("\n")
      .filter(!_.isBlank)
      .mkString("\n")
  }

  /**
   * Merge with hocon config in the ZLayer context.
   */
  def resolveConfig: ZIO[Config, Throwable, Config] =
    for {
      originConfig    <- ZIO.service[Config]
      generatedConfig <- toRawAkkaConfig
      _               <- ZIO.logInfo(s"Generate akka config:\n$generatedConfig").when(logGeneratedConfig)
      resolvedConfig  <- ZIO.attempt(ConfigFactory.parseString(generatedConfig).withFallback(originConfig))
    } yield resolvedConfig

object AkkaConf:

  private def when(cond: Boolean)(s: => String) = if cond then s else ""
  private def wrapQuote(str: String)            = s"\"$str\""

  /**
   * Extract AkkaConf from root hocon config.
   */
  val live: ZLayer[Config, Throwable, AkkaConf] = ZLayer {
    for {
      source <- HoconConfig.hoconSource("potamoi.akka")
      config <- read(descriptor[AkkaConf].from(source))
    } yield config
  }

  /**
   * Local single node, using for testing.
   */
  def local(nodeRoles: List[String] = List.empty): ULayer[AkkaConf] =
    ZLayer.succeed {
      AkkaConf(
        arteryHost = Some("127.0.0.1"),
        arteryPort = 3300,
        seedAddresses = List("127.0.0.1:3300"),
        nodeRoles = nodeRoles
      )
    }

  /**
   * Local multiple nodes cluster, using for testing.
   */
  def localCluster(arteryPort: Int, seedPorts: List[Int], nodeRoles: List[String] = List.empty): ULayer[AkkaConf] =
    ZLayer.succeed {
      AkkaConf(
        arteryHost = Some("127.0.0.1"),
        arteryPort = arteryPort,
        seedAddresses = seedPorts.map(s => s"127.0.0.1:$s"),
        nodeRoles = nodeRoles
      )
    }
