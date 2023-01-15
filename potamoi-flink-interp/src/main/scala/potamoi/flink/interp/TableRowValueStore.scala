package potamoi.flink.interp

import potamoi.flink.interp.model.{ResultDropStrategy, ResultStoreConf, RowValue}
import potamoi.flink.interp.model.ResultDropStrategy.*
import zio.{Ref, UIO, ZIO}
import potamoi.zios.runNow
import potamoi.syntax.tap

import scala.collection.mutable.ListBuffer

/**
 * Fiber-safe bounded linear store for flink table row value.
 */
object TableRowValueStore:
  def make(capacity: Int, dropStg: ResultDropStrategy): UIO[TableRowValueStore] =
    dropStg match
      case DropTail => ZIO.succeed(DroppingTableRowValueStore(capacity))
      case DropHead => ZIO.succeed(SlidingTableRowValueStore(capacity))

sealed trait TableRowValueStore:
  def handleId: UIO[String]
  def bindHandleId(handleId: String): UIO[Unit]
  def collect(row: RowValue): UIO[Unit]
  def snapshot: UIO[List[RowValue]]
  def slice(from: Int, until: Int): UIO[List[RowValue]]
  def clear: UIO[Unit]
  def size: UIO[Int]

sealed abstract private class ListBasedTableRowValueStore(capacity: Int) extends TableRowValueStore:
  protected val listRef                                          = Ref.make[ListBuffer[RowValue]](ListBuffer.empty).runNow
  protected val handleIdRef                                      = Ref.make[String]("").runNow
  override def handleId: UIO[String]                             = handleIdRef.get
  override def bindHandleId(handleId: String): UIO[Unit]         = handleIdRef.set(handleId)
  override def snapshot: UIO[List[RowValue]]                     = listRef.get.map(_.toList)
  override def slice(from: Int, until: Int): UIO[List[RowValue]] = listRef.get.map(_.slice(from, until).toList)
  override def size: UIO[Int]                                    = listRef.get.map(_.size)
  override def clear: UIO[Unit]                                  = listRef.update(_.tap(_.clear()))

class DroppingTableRowValueStore(capacity: Int) extends ListBasedTableRowValueStore(capacity):
  override def collect(row: RowValue): UIO[Unit] = listRef.update { s =>
    if s.size < capacity then s += row
    else s
  }

case class SlidingTableRowValueStore[A](capacity: Int) extends ListBasedTableRowValueStore(capacity):
  override def collect(row: RowValue): UIO[Unit] = listRef.update { s =>
    if s.size < capacity then s += row
    else (s -= s.head) += row
  }
