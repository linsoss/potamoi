package com.github.potamois.potamoi.akka.testkit

import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.{Cluster, MemberStatus}
import com.github.potamois.potamoi.commons.PotaConfig.RichConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.language.postfixOps

/**
 * Akka cluster node specification test trait for use alongside
 * [[akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit]].
 *
 * This is actually simulated by running multiple ActorSystem in
 * the same jvm for better debugging in the IDE than sbt-multi-jvm
 * plugin.
 *
 * @author Al-assad
 */
trait STAkkaClusterMockSpec extends STAkkaSpec {

  val nodeAkkaName = "potamoi-mock"

  /**
   * ActorSystem creation behavior and remote canonical port.
   */
  case class Role[T](guardian: Behavior[T], port: Int)

  /**
   * Launch a cluster consisting of several specified ActorSystem.
   */
  def launchCluster[T](nodes: Role[T]*): Map[Role[T], ActorSystem[T]] = {
    val seedPorts = nodes.map(_.port)
    nodes.map { node =>
      node -> ActorSystem(node.guardian, nodeAkkaName, genClusterConfig(node.port, seedPorts))
    }.toMap
  }

  private def genClusterConfig(port: Int, seedPorts: Seq[Int]): Config = {
    val seedContent = seedPorts.map(p => s""""akka://$nodeAkkaName@127.0.0.1:$p"""".stripMargin).mkString(",")
    ConfigFactory.parseString(
      s"""akka {
         |  actor.provider = "cluster"
         |  remote.artery {
         |     canonical.hostname = "127.0.0.1"
         |     canonical.port = $port
         |  }
         |  cluster {
         |    seed-nodes = [ $seedContent ]
         |    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
         |    jmx.multi-mbeans-in-same-jvm = on
         |  }
         |  log-dead-letters = 0
         |  log-dead-letters-during-shutdown = off
         |}
         |""".stripMargin)
      .ensurePotamoi
  }

  /**
   * Blocking unit all ActorSystem is UP.
   */
  def waitAllNodeUp(systems: Seq[ActorSystem[_]], waitTimeout: FiniteDuration = 60.seconds): Unit = {
    systems.map(Cluster(_))
      .foreach(cluster =>
        eventually(timeout(waitTimeout), interval(500 millis)) {
          cluster.selfMember.status shouldBe MemberStatus.Up
        }
      )
    log.info("[@potamoi-mock] all nodes are UP")
  }


  implicit class ClusterNodesImplicits[T](clusterNodes: Map[Role[T], ActorSystem[T]]) {

    // Blocking unit all ActorSystem is UP.
    def waitAllUp(timeout: FiniteDuration): Map[Role[T], ActorSystem[T]] = {
      waitAllNodeUp(clusterNodes.values.toSeq, timeout)
      clusterNodes
    }
    def waitAllUp: Map[Role[T], ActorSystem[T]] = waitAllUp(60.seconds)

    // Shutdown all ActorSystem
    def shutdown(): Unit = {
      clusterNodes.values.foreach(_.terminate())
    }
  }

}
