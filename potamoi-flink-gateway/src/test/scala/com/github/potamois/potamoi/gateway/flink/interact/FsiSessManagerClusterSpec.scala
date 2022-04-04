package com.github.potamois.potamoi.gateway.flink.interact

import akka.Done
import akka.actor.Address
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import com.github.potamois.potamoi.akka.testkit.{STAkkaClusterMockSpec, defaultClusterConfig}
import com.github.potamois.potamoi.commons.FutureImplicits.Wrapper
import com.github.potamois.potamoi.gateway.flink.interact.FsiExecutor.{ExecuteSqls, MaybeDone}

/**
 * Testing the behavior of [[FsiSessManager]] on multiple nodes in a cluster.
 *
 * @note The following cases would take a long time.
 * @author Al-assad
 */
// noinspection TypeAnnotation
class FsiSessManagerClusterSpec extends ScalaTestWithActorTestKit(defaultClusterConfig) with STAkkaClusterMockSpec {

  import FsiMockExecutor._
  import FsiSessManager._
  import NodeFsiSessObserver._

  val nodeWatcher = spawn(NodeFsiSessObserver())

  val role14a = Role(FsiSessManager(
    flinkVerSign = 114,
    fsiExecutorBehavior = FsiMockExecutor.apply(_, nodeWatcher)
  ), 25251)

  val role13 = Role(FsiSessManager(
    flinkVerSign = 113,
    fsiExecutorBehavior = FsiMockExecutor.apply(_, nodeWatcher)
  ), 25252)

  val role14b = Role(FsiSessManager(
    flinkVerSign = 114,
    fsiExecutorBehavior = FsiMockExecutor.apply(_, nodeWatcher)
  ), 25253)


  "cluster case-1: create session -> forward command -> terminate session" in {
    val cluster = launchCluster(role14a, role13, role14b).waitAllUp.printMembers

    // create 114-type, 113-type session from role14a
    val sessId1 = (cluster(role14a) ? (CreateSession(114, _))).waitResult.getOrElse(fail)
    val sessId2 = (cluster(role14a) ? (CreateSession(114, _))).waitResult.getOrElse(fail)
    val sessId3 = (cluster(role14a) ? (CreateSession(113, _))).waitResult.getOrElse(fail)

    // sessId3 should be created in role13
    investCluster(sessId3) { case (addr, msg) =>
      msg shouldBe Created
      addr shouldBe cluster(role13).address
    }
    // sessId1 should be created in role14a or role14b
    val sessId1Addr = investCluster(sessId1) { case (addr, msg) =>
      msg shouldBe Created
      addr should (be(cluster(role14a).address) or be(cluster(role14b).address))
    }
    // sessId2 should be created in role14a or role14b
    val sessId2Addr = investCluster(sessId2) { case (addr, msg) =>
      msg shouldBe Created
      addr should (be(cluster(role14a).address) or be(cluster(role14b).address))
    }
    // sessId1 and sessId2 should be in different node
    sessId1Addr should not be sessId2Addr

    // create 114-type executor from role13
    val sessId4 = (cluster(role13) ? (CreateSession(114, _))).waitResult.getOrElse(fail)
    // sessId4 should be created in role14a or role14b
    investCluster(sessId4) { case (addr, msg) =>
      msg shouldBe Created
      addr should (be(cluster(role14a).address) or be(cluster(role14b).address))
    }

    // exist session
    cluster.values.foreach { sys =>
      probeRef[Boolean](sys ! ExistSession(sessId1, _)) expectMessage true
      probeRef[Boolean](sys ! ExistSession(sessId2, _)) expectMessage true
      probeRef[Boolean](sys ! ExistSession(sessId3, _)) expectMessage true
    }

    // forward command to sessId1 from role14a
    //    probeRef[MaybeDone](executor1 ! ExecuteSqls("sql-1", ExecProps(), _)) expectMessage Right(Done)
    //    investCluster(sessId1) { case (addr, msg) =>
    //      msg shouldBe "sql-1"
    //      addr shouldBe sessId1Addr
    //    }

    // forward command to sessId2 from role13
    probeRef[MaybeDone](cluster(role14a) ! sessId2 -> ExecuteSqls("sql-2", ExecProps(), _)) expectMessage Right(Done)
    investCluster(sessId2) { case (addr, msg) =>
      msg shouldBe "sql-2"
      addr shouldBe sessId2Addr
    }

    // forward command to sessId3 from role14b
    //    probeRef[MaybeDone](executor3 ! ExecuteSqls("sql-3", ExecProps(), _)) expectMessage Right(Done)
    //    investCluster(sessId3) { case (addr, msg) =>
    //      msg shouldBe "sql-3"
    //      addr shouldBe cluster(role13).address
    //    }

    // todo terminate session


    cluster.shutdown()
    nodeWatcher ! Clear
  }


  "cluster case-2: fault tolerance when nodes go offline" in {
  }


  def investCluster(sessId: String)(assert: (Address, String) => Any): Address =
    probeRef[Option[(Address, String)]](nodeWatcher ! GetSessHistory(sessId, _)).receivePF {
      case None => fail
      case Some((addr, msg)) =>
        assert(addr, msg)
    }.map(_._1).getOrElse(fail)


}


