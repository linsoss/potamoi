package potamoi.flink.storage

import potamoi.flink.{FlinkDataStoreErr, FlinkMajorVer}
import potamoi.flink.model.interact.{InteractSession, InteractSessionStatus, InterpreterPod}
import potamoi.flink.storage.InteractSessionStorage.{PodStorage, SessionStorage}
import zio.IO
import zio.stream.Stream

/**
 * Flink interactive session storage.
 */
trait InteractSessionStorage:
  def session: SessionStorage
  def pod: PodStorage

object InteractSessionStorage {

  /**
   * Interactive session.
   */
  trait SessionStorage extends SessionStorage.Modify with SessionStorage.Query

  object SessionStorage:
    trait Modify:
      def put(session: InteractSession): IO[FlinkDataStoreErr, Unit]
      def rm(session: InteractSession): IO[FlinkDataStoreErr, Unit]

    trait Query:
      def get(sessionId: String): IO[FlinkDataStoreErr, Option[InteractSession]]
      def list: Stream[FlinkDataStoreErr, InteractSession]
      def listSessionId: IO[FlinkDataStoreErr, List[String]]

  /**
   * Remote interpreter pod.
   */
  trait PodStorage extends PodStorage.Modify with PodStorage.Query

  object PodStorage:
    trait Modify:
      def put(pod: InterpreterPod): IO[FlinkDataStoreErr, Unit]
      def rm(flinkVer: FlinkMajorVer, address: String, port: Int): IO[FlinkDataStoreErr, Unit]

    trait Query:
      def list: IO[FlinkDataStoreErr, List[InterpreterPod]]
      def listFlinkVersion: IO[FlinkDataStoreErr, Set[FlinkMajorVer]]
      def exists(flinkVer: FlinkMajorVer): IO[FlinkDataStoreErr, Boolean]

}
