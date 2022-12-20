package potamoi.flink.observer

import com.devsisters.shardcake.Sharding
import potamoi.common.Syntax.toPrettyString
import potamoi.flink.FlinkConf
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.storage.FlinkSnapshotStorage
import potamoi.kubernetes.{K8sConf, K8sOperator}
import potamoi.logger.PotaLogger
import potamoi.sharding.{ShardingConf, Shardings}
import zio.{durationInt, IO, ZIO}
import potamoi.zios.*
import potamoi.syntax.*
import potamoi.errs.{headMessage, recurse}
import zio.Schedule.spaced
import potamoi.flink.{watch, watchPretty, watchPrettyTag}
import potamoi.flink.model.FlK8sComponentName.jobmanager
import zio.Console.printLine

object FlinkObserverTest {

  def testing[A](effect: FlinkObserver => IO[Throwable, A]): Unit = {
    val program = ZIO
      .serviceWithZIO[FlinkObserver] { obr =>
        obr.manager.registerEntities *>
        Sharding.registerScoped.ignore *>
        effect(obr)
      }
      .tapErrorCause(cause => ZIO.logErrorCause(cause.headMessage, cause.recurse))
    ZIO
      .scoped(program)
      .provide(
        FlinkConf.test.resolve("var/potamoi").asLayer,
        K8sConf.default.asLayer,
        K8sOperator.live,
        FlinkSnapshotStorage.test,
        ShardingConf.test.asLayer,
        Shardings.test,
        FlinkObserver.live
      )
      .provideLayer(PotaLogger.default)
      .run
      .exitCode
  }

  val fcid1: Fcid = "app-t1"     -> "fdev"
  val fcid2: Fcid = "app-t2"     -> "fdev"
  val fcid3: Fcid = "session-01" -> "fdev"
}

import FlinkObserverTest.*
import FlinkObserver.*

// ------------------------------- test start -------------------------------------

object TestTrackerManager:

  @main def trackAndUnTrack = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.manager.track(fcid2) *>
    obr.manager.isBeTracked(fcid1).map(println) *>
    obr.manager.isBeTracked(fcid3).map(println) *>
    obr.manager.listTrackedClusters.runCollect.map(println) *>
    obr.manager.untrack(fcid1) *>
    obr.manager.listTrackedClusters.runCollect.map(println)
  }

  @main def viewTrackerStatus = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.manager.getTrackersStatus(fcid1).watchPretty
  }

  @main def streamViewTackerStatus = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.manager.track(fcid2) *>
    obr.manager.track(fcid3) *>
    obr.manager.listTrackersStatus(3).runCollect.watchPretty
  }

object TestFlinkRestEndpointQuery:

  @main def testRetrieve = testing { obr =>
    obr.restEndpoint.retrieve(fcid1).map(_.map(_.toPrettyStr)).debug
  }

  @main def testGetEnsure = testing { obr =>
    obr.restEndpoint.getEnsure(fcid1).map(_.map(_.toPrettyStr)).debug *>
    obr.restEndpoint.getEnsure(fcid1).map(_.map(_.toPrettyStr)).debug
  }

  @main def testGet = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.restEndpoint.get(fcid1).watchPretty
  }

object TestFlinkClusterQuery:

  @main def testViewOverview = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.cluster.overview.get(fcid1).watchPretty
  }

  @main def testViewTmDetail = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.cluster.tmDetail.list(fcid1).watchPrettyTag("tm-detail").fork *>
    obr.cluster.tmDetail.listTmId(fcid1).watchPrettyTag("tm-id").fork *>
    ZIO.never
  }

  @main def testViewJmMetrics = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.cluster.jmMetrics.get(fcid1).watchPretty
  }

  @main def testViewTmMetrics = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.cluster.tmMetrics.list(fcid1).watch
  }

object TestFlinkJobQuery:

  @main def testViewJobOverview = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.job.overview.list(fcid1).watchPrettyTag("overview").fork *>
    obr.job.overview.listJobId(fcid1).watchPrettyTag("job-id").fork *>
    obr.job.overview.listJobState(fcid1).watchPrettyTag("job-state").fork *>
    ZIO.never
  }

  @main def testViewJobMetrics = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.job.metrics.list(fcid1).watchPretty
  }

object TestFlinkK8sRefQuery:

  @main def testViewRefSnap = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.k8s.getRef(fcid1).watchPretty.fork *>
    obr.k8s.getRefSnap(fcid1).watchPretty.fork *>
    ZIO.never
  }

  @main def testViewSpec = testing { obr =>
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

  @main def testViewPodMetrics = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.k8s.podMetrics.list(fcid1).watchPretty
  }

  @main def testScanK8sNamespace = testing { obr =>
    obr.k8s.scanK8sNamespace("fdev").runForeach(fcid => printLine(fcid.toPrettyStr))
  }

  @main def testViewLog = testing { obr =>
    obr.manager.track(fcid1) *>
    obr.k8s.pod
      .list(fcid1)
      .repeatUntil { pods => pods.exists(e => e.component == jobmanager) }
      .map(pods => pods.find(e => e.component == jobmanager).get.name)
      .flatMap { podName =>
        obr.k8s.viewLog(fcid1, podName, follow = true).map(println).runDrain
      }
  }
