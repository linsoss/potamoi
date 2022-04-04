package com.github.potamois.potamoi.akka.testkit

import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.MemberStatus
import akka.cluster.typed.{Cluster, Join}
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

  def actorSystemName: String = getClass.getName.split('.').last

  /**
   * ActorSystem creation behavior and remote canonical port.
   */
  case class Role[T](guardian: Behavior[T], port: Int)

  /**
   * Launch a cluster consisting of several specified ActorSystem.
   */
  def launchCluster[T](nodes: Role[T]*)
                      (implicit observerSystem: ActorSystem[_]): Map[Role[T], ActorSystem[T]] = {
    val seedPorts = nodes.map(_.port)
    val cluster = nodes.map { node =>
      node -> ActorSystem(node.guardian, actorSystemName, genClusterConfig(node.port, seedPorts))
    }.toMap
    if (shouldObserverJoinCluster) Cluster(observerSystem).manager ! Join(cluster.head._2.address)
    cluster
  }

  private def shouldObserverJoinCluster(implicit observerSystem: ActorSystem[_]): Boolean =
    observerSystem.settings.config.hasPath("akka.actor.provider") &&
    observerSystem.settings.config.getString("akka.actor.provider") == "cluster"


  private def genClusterConfig(port: Int, seedPorts: Seq[Int]): Config = {
    val seedContent = seedPorts.map(p => s""""akka://$actorSystemName@127.0.0.1:$p"""".stripMargin).mkString(",")
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
  def waitAllNodeUp(systems: Seq[ActorSystem[_]], waitTimeout: FiniteDuration = 60.seconds)
                   (implicit originSystem: ActorSystem[_]): Unit = {
    systems.map(Cluster(_))
      .foreach(cluster =>
        eventually(timeout(waitTimeout), interval(500 millis)) {
          cluster.selfMember.status shouldBe MemberStatus.Up
        }
      )
    if (shouldObserverJoinCluster)
      eventually(timeout(waitTimeout), interval(500 millis)) {
        Cluster(originSystem).selfMember.status shouldBe MemberStatus.Up
      }
    log.info("[@potamoi-mock] all nodes are UP")
  }


  implicit class ClusterNodesImplicits[T](clusterNodes: Map[Role[T], ActorSystem[T]])(implicit observerSystem: ActorSystem[_]) {

    // Blocking unit all ActorSystem is UP.
    def waitAllUp(timeout: FiniteDuration): Map[Role[T], ActorSystem[T]] = {
      waitAllNodeUp(clusterNodes.values.toSeq, timeout)
      clusterNodes
    }
    def waitAllUp: Map[Role[T], ActorSystem[T]] = waitAllUp(60.seconds)

    // println all members
    def printMembers: Map[Role[T], ActorSystem[T]] = {
      val observer = if (shouldObserverJoinCluster) observerSystem else clusterNodes.head._2
      log.info(
        s"""[@potamoi-mock] all members:
           |${Cluster(observer).state.members.map("\t" + _.toString()).mkString("\n")}""".stripMargin)
      clusterNodes
    }
    // Shutdown all ActorSystem
    def shutdown(): Unit = {
      clusterNodes.values.foreach(_.terminate())
    }
  }

}
