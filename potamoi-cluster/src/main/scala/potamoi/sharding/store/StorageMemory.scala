package potamoi.sharding.store

import com.devsisters.shardcake.interfaces.Storage
import com.devsisters.shardcake.{Pod, PodAddress, ShardId, ShardManagerClient}
import zio.{durationInt, Ref, Schedule, Task, ZIO, ZIOAppDefault, ZLayer}
import zio.managed.ZManaged
import zio.stream.{SubscriptionRef, ZStream}

/**
 * Enhance shardcake memory store.
 */
object StorageMemory:

  val live: ZLayer[ShardManagerClient, Nothing, Storage] = ZLayer {
    for {
      shardManager   <- ZIO.service[ShardManagerClient]
      assignmentsRef <- SubscriptionRef.make(Map.empty[ShardId, Option[PodAddress]])
      podsRef        <- Ref.make(Map.empty[PodAddress, Pod])
      _              <- ZStream
                          .fromZIO(shardManager.getAssignments)
                          .repeat(Schedule.spaced(100.millis))
                          .zipWithPrevious
                          .filter { case (prev, cur) => !prev.contains(cur) }
                          .map(_._2)
                          .mapZIO(assignmentsRef.set)
                          .runDrain
                          .forkDaemon
    } yield new Storage {
      def getAssignments: Task[Map[ShardId, Option[PodAddress]]]                       = assignmentsRef.get
      def saveAssignments(assignments: Map[ShardId, Option[PodAddress]]): Task[Unit]   = assignmentsRef.set(assignments)
      def assignmentsStream: ZStream[Any, Throwable, Map[ShardId, Option[PodAddress]]] = assignmentsRef.changes
      def getPods: Task[Map[PodAddress, Pod]]                                          = podsRef.get
      def savePods(pods: Map[PodAddress, Pod]): Task[Unit]                             = podsRef.set(pods)
    }
  }
