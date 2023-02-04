package potamoi.flink.observer

import org.scalatest.wordspec.AnyWordSpec
import potamoi.{BaseConf, HoconConfig, NodeRoles, PotaErr}
import potamoi.akka.{AkkaMatrix, AkkaConf}
import potamoi.common.Syntax.toPrettyString
import potamoi.debugs.*
import potamoi.flink.*
import potamoi.flink.model.Fcid
import potamoi.flink.model.snapshot.FlK8sComponentName.jobmanager
import potamoi.flink.observer.FlinkObserverTest.fcid1
import potamoi.flink.storage.FlinkDataStorage
import potamoi.kubernetes.{K8sConf, K8sOperator}
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.syntax.*
import potamoi.zios.*
import zio.{durationInt, IO, Scope, ZIO, ZIOAppDefault, ZLayer}
import zio.Console.printLine
import zio.Schedule.spaced
import potamoi.common.TimeExtension.given_Conversion_ZIODuration_ScalaDuration
import potamoi.flink.model.snapshot.FlinkRestSvcEndpoint

object FlinkObserverTest:

  val testInMultiNode = false

  val fcid1: Fcid = "app-t1"     -> "fdev"
  val fcid2: Fcid = "app-t2"     -> "fdev"
  val fcid3: Fcid = "session-01" -> "fdev"

  def testing[E, A](effect: FlinkObserver => IO[E, A]): Unit = {
    ZIO
      .serviceWithZIO[FlinkObserver] { obr => effect(obr) }
      .tapErrorCause(cause => ZIO.logErrorCause(cause))
      .provide(
        HoconConfig.empty,
        LogConf.default,
        BaseConf.test,
        FlinkConf.test,
        K8sConf.default,
        K8sOperator.live,
        FlinkDataStorage.test,
        FlinkObserver.live,
        if !testInMultiNode then AkkaConf.local(List(NodeRoles.flinkService))
        else AkkaConf.localCluster(3301, List(3301, 3302)).project(_.copy(defaultAskTimeout = 15.seconds)),
        AkkaMatrix.live,
        Scope.default,
      )
      .provideLayer(PotaLogger.default)
      .run
      .exitCode
  }

  @main def launchRemoteFlinkObserverApp: Unit = {
    for {
      _ <- AkkaMatrix.active
      _ <- FlinkObserver.active
      _ <- ZIO.never
    } yield ()
  }
    .provide(
      HoconConfig.empty,
      LogConf.default,
      BaseConf.test,
      FlinkConf.test,
      K8sConf.default,
      K8sOperator.live,
      FlinkDataStorage.test,
      FlinkObserver.live,
      AkkaConf.localCluster(3302, List(3301, 3302), List(NodeRoles.flinkService)).project(_.copy(defaultAskTimeout = 15.seconds)),
      AkkaMatrix.live,
      Scope.default,
    )
    .provideLayer(PotaLogger.default)
    .run
    .exitCode

import potamoi.flink.observer.FlinkObserver.*
import potamoi.flink.observer.FlinkObserverTest.*

// ------------------------------- test start -------------------------------------

// @Ignore
class TestTrackerManager extends AnyWordSpec:

  "track and untrack" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.manager.track(fcid2) *>
    obr.manager.isBeTracked(fcid1).map(println) *>
    obr.manager.isBeTracked(fcid3).map(println) *>
    obr.manager.listTrackedClusters.runCollect.map(println) *>
    obr.manager.untrack(fcid1) *>
    obr.manager.listTrackedClusters.runCollect.map(println)
  }

  "view tracker status" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.manager.getTrackersStatus(fcid1).watchPretty
  }

  "watch tacker status" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.manager.track(fcid2) *>
    obr.manager.track(fcid3) *>
    obr.manager.listTrackersStatus(3).runCollect.watchPretty
  }

// @Ignore
class TestFlinkRestEndpointQuery extends AnyWordSpec:

  "retrieve endpoint" in testing { obr =>
    obr.restEndpoint.retrieve(fcid1).map(_.map(_.toPrettyStr)).debug
  }

  "getEnsure endpoint" in testing { obr =>
    obr.restEndpoint.getEnsure(fcid1).map(_.map(_.toPrettyStr)).debug *>
    obr.restEndpoint.getEnsure(fcid1).map(_.map(_.toPrettyStr)).debug
  }

  "get endpoint" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.restEndpoint.get(fcid1).watchPretty
  }

// @Ignore
class TestFlinkClusterQuery extends AnyWordSpec:

  "view overview" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.cluster.overview.get(fcid1).watchPretty
  }

  "view taskmanager detail" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.cluster.tmDetail.list(fcid1).watchPrettyTag("tm-detail").fork *>
    obr.cluster.tmDetail.listTmId(fcid1).watchPrettyTag("tm-id").fork *>
    ZIO.never
  }

  "view jobmanager metrics" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.cluster.jmMetrics.get(fcid1).watchPretty
  }

  "view taskmanager metrics" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.cluster.tmMetrics.list(fcid1).watch
  }

// @Ignore
class TestFlinkJobQuery extends AnyWordSpec:

  "view job overview" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.job.overview.list(fcid1).watchPrettyTag("overview").fork *>
    obr.job.overview.listJobId(fcid1).watchPrettyTag("job-id").fork *>
    obr.job.overview.listJobState(fcid1).watchPrettyTag("job-state").fork *>
    ZIO.never
  }

  "view job metrics" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.job.metrics.list(fcid1).watchPretty
  }

// @Ignore
class TestFlinkK8sRefQuery extends AnyWordSpec:

  "view K8sRefSnap" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.k8s.getRef(fcid1).watchPretty.fork *>
    obr.k8s.getRefSnap(fcid1).watchPretty.fork *>
    ZIO.never
  }

  "view K8sSpec" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.k8s
      .getRef(fcid1)
      .repeatUntil { snap =>
        snap.deployment.nonEmpty && snap.service.nonEmpty && snap.pod.nonEmpty && snap.configMap.nonEmpty
      }
      .flatMap { snap =>
        obr.k8s.deployment.getSpec(fcid1, snap.deployment.head).debugPretty <&>
        obr.k8s.service.getSpec(fcid1, snap.service.head).debugPretty <&>
        obr.k8s.pod.getSpec(fcid1, snap.pod.head).debugPretty <&>
        obr.k8s.configmap.getData(fcid1, snap.configMap.head).debugPretty
      }
  }

  "view PodMetrics" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.k8s.podMetrics.list(fcid1).watchPretty
  }

  "scan K8s namespace" in testing { obr =>
    obr.k8s.scanK8sNamespace("fdev").runForeach(fcid => printLine(fcid.toPrettyStr)).ignore
  }

  "view k8s pod log" in testing { obr =>
    obr.manager.track(fcid1) *>
    obr.k8s.pod
      .list(fcid1)
      .repeatUntil { pods => pods.exists(e => e.component == jobmanager) }
      .map(pods => pods.find(e => e.component == jobmanager).get.name)
      .flatMap { podName =>
        obr.k8s.viewLog(fcid1, podName, follow = true).map(println).runDrain
      }
  }
