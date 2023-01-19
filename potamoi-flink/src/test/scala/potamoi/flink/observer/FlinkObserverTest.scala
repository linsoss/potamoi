package potamoi.flink.observer

import com.devsisters.shardcake.Sharding
import org.scalatest.wordspec.AnyWordSpec
import potamoi.common.Syntax.toPrettyString
import potamoi.PotaErr
import potamoi.debugs.*
import potamoi.flink.*
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.model.FlK8sComponentName.jobmanager
import potamoi.flink.storage.FlinkSnapshotStorage
import potamoi.kubernetes.{K8sConf, K8sOperator}
import potamoi.logger.PotaLogger
import potamoi.sharding.{LocalShardManager, ShardingConf, Shardings}
import potamoi.sharding.LocalShardManager.withLocalShardManager
import potamoi.syntax.*
import potamoi.zios.*
import zio.{durationInt, IO, ZIO}
import zio.Console.printLine
import zio.Schedule.spaced

object FlinkObserverTest:

  def testing[E, A](effect: FlinkObserver => IO[E, A]): Unit = {
    val program = ZIO
      .serviceWithZIO[FlinkObserver] { obr =>
        obr.manager.registerEntities *>
        Sharding.registerScoped.ignore *>
        effect(obr)
      }
      .tapErrorCause(cause => ZIO.logErrorCause(cause))
    ZIO
      .scoped(program)
      .provide(
        FlinkConfTest.asLayer,
        K8sConfTest.asLayer,
        K8sOperator.live,
        FlinkSnapshotStorage.test,
        ShardingConf.test.asLayer,
        Shardings.test,
        FlinkObserver.live
      )
      .withLocalShardManager
      .provideLayer(PotaLogger.default)
      .run
      .exitCode
  }

  val fcid1: Fcid = "app-t1"     -> "fdev"
  val fcid2: Fcid = "app-t2"     -> "fdev"
  val fcid3: Fcid = "session-01" -> "fdev"

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
