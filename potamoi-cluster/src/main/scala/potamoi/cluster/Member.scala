package potamoi.cluster

import akka.cluster
import potamoi.cluster.MemberStatuses.given_JsonCodec_MemberStatus
import potamoi.codecs
import zio.json.JsonCodec

/**
 * Mirror of [[akka.cluster.Member]]
 */
case class Member(nodeUid: Long, address: Address, status: MemberStatus, roles: Set[String], appVersion: String) derives JsonCodec

object Member:
  
  def apply(member: cluster.Member): Member = Member(
    nodeUid = member.uniqueAddress.longUid,
    address = Address(member.uniqueAddress.address),
    status = MemberStatuses(member.status),
    roles = member.roles,
    appVersion = member.appVersion.version
  )

/**
 * Mirror of [[akka.cluster.MemberStatus]]
 */
enum MemberStatus:
  case Joining, WeaklyUp, Up, Leaving, Exiting, Down, Removed, PreparingForShutdown, ReadyForShutdown

object MemberStatuses:

  given JsonCodec[MemberStatus] = codecs.simpleEnumJsonCodec(MemberStatus.values)

  def apply(status: cluster.MemberStatus): MemberStatus = status match {
    case cluster.MemberStatus.Joining              => MemberStatus.Joining
    case cluster.MemberStatus.WeaklyUp             => MemberStatus.WeaklyUp
    case cluster.MemberStatus.Up                   => MemberStatus.Up
    case cluster.MemberStatus.Leaving              => MemberStatus.Leaving
    case cluster.MemberStatus.Exiting              => MemberStatus.Exiting
    case cluster.MemberStatus.Down                 => MemberStatus.Down
    case cluster.MemberStatus.Removed              => MemberStatus.Removed
    case cluster.MemberStatus.PreparingForShutdown => MemberStatus.PreparingForShutdown
    case cluster.MemberStatus.ReadyForShutdown     => MemberStatus.ReadyForShutdown
  }

  def toAkka(status: MemberStatus): cluster.MemberStatus = status match {
    case MemberStatus.Joining              => cluster.MemberStatus.Joining
    case MemberStatus.WeaklyUp             => cluster.MemberStatus.WeaklyUp
    case MemberStatus.Up                   => cluster.MemberStatus.Up
    case MemberStatus.Leaving              => cluster.MemberStatus.Leaving
    case MemberStatus.Exiting              => cluster.MemberStatus.Exiting
    case MemberStatus.Down                 => cluster.MemberStatus.Down
    case MemberStatus.Removed              => cluster.MemberStatus.Removed
    case MemberStatus.PreparingForShutdown => cluster.MemberStatus.PreparingForShutdown
    case MemberStatus.ReadyForShutdown     => cluster.MemberStatus.ReadyForShutdown
  }
