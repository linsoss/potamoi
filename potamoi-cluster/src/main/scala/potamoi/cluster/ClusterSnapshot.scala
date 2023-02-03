package potamoi.cluster

import akka.cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import zio.json.JsonCodec

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/**
 * Mirror of [[akka.cluster.ClusterEvent.CurrentClusterState]]
 */
case class ClusterSnapshot(
    self: Member,
    members: Set[Member],
    unreachable: Set[Member],
    seenBy: Set[Address],
    leader: Option[Address],
    roleLeaderMap: Map[String, Option[Address]],
    unreachableDataCenters: Set[String])
    derives JsonCodec:

  def allRoles: Set[String] = roleLeaderMap.keySet

object ClusterSnapshot:

  def apply(selfMember: cluster.Member, clusterState: CurrentClusterState): ClusterSnapshot = ClusterSnapshot(
    self = Member(selfMember),
    members = clusterState.getMembers.asScala.map(Member(_)).toSet,
    unreachable = clusterState.unreachable.map(Member(_)),
    seenBy = clusterState.seenBy.map(Address(_)),
    leader = clusterState.leader.map(Address(_)),
    roleLeaderMap = clusterState.roleLeaderMap.map { case (k, v) => k -> v.map(Address(_)) },
    unreachableDataCenters = clusterState.unreachableDataCenters
  )
