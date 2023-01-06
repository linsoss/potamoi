package potamoi.flink.model

import potamoi.curTs
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink jobmanager metrics.
 * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics
 */
case class FlinkJmMetrics(
    clusterId: String,
    namespace: String,
    jvmClassLoaderClassesLoaded: Option[Long] = None,
    jvmClassLoaderClassesUnloaded: Option[Long] = None,
    jvmCpuLoad: Option[Double] = None,
    jvmCpuTime: Option[Long] = None,
    jvmGarbageCollectorCopyCount: Option[Long] = None,
    jvmGarbageCollectorCopyTime: Option[Long] = None,
    jvmGarbageCollectorMarkSweepCompactCount: Option[Long] = None,
    jvmGarbageCollectorMarkSweepCompactTime: Option[Long] = None,
    jvmMemoryDirectCount: Option[Long] = None,
    jvmMemoryDirectMemoryUsed: Option[Long] = None,
    jvmMemoryDirectTotalCapacity: Option[Long] = None,
    jvmMemoryHeapCommitted: Option[Long] = None,
    jvmMemoryHeapMax: Option[Long] = None,
    jvmMemoryHeapUsed: Option[Long] = None,
    jvmMemoryMappedCount: Option[Long] = None,
    jvmMemoryMappedMemoryUsed: Option[Long] = None,
    jvmMemoryMappedTotalCapacity: Option[Long] = None,
    jvmMemoryMetaspaceCommitted: Option[Long] = None,
    jvmMemoryMetaspaceMax: Option[Long] = None,
    jvmMemoryMetaspaceUsed: Option[Long] = None,
    jvmMemoryNonHeapCommitted: Option[Long] = None,
    jvmMemoryNonHeapMax: Option[Long] = None,
    jvmMemoryNonHeapUsed: Option[Long] = None,
    jvmThreadsCount: Option[Long] = None,
    numPendingTaskManagers: Option[Long] = None,
    numRegisteredTaskManagers: Option[Long] = None,
    numRunningJobs: Option[Long] = None,
    startWorkFailurePerSecond: Option[Double] = None,
    taskSlotsAvailable: Option[Long] = None,
    taskSlotsTotal: Option[Long] = None,
    ts: Long = curTs)
    derives JsonCodec

object FlinkJmMetrics:

  val metricsRawKeys: Set[String] = Set(
    "Status.JVM.CPU.Load",
    "Status.JVM.CPU.Time",
    "Status.JVM.ClassLoader.ClassesLoaded",
    "Status.JVM.ClassLoader.ClassesUnloaded",
    "Status.JVM.GarbageCollector.Copy.Count",
    "Status.JVM.GarbageCollector.Copy.Time",
    "Status.JVM.GarbageCollector.MarkSweepCompact.Count",
    "Status.JVM.GarbageCollector.MarkSweepCompact.Time",
    "Status.JVM.Memory.Direct.Count",
    "Status.JVM.Memory.Direct.MemoryUsed",
    "Status.JVM.Memory.Direct.TotalCapacity",
    "Status.JVM.Memory.Heap.Committed",
    "Status.JVM.Memory.Heap.Max",
    "Status.JVM.Memory.Heap.Used",
    "Status.JVM.Memory.Mapped.Count",
    "Status.JVM.Memory.Mapped.MemoryUsed",
    "Status.JVM.Memory.Mapped.TotalCapacity",
    "Status.JVM.Memory.Metaspace.Committed",
    "Status.JVM.Memory.Metaspace.Max",
    "Status.JVM.Memory.Metaspace.Used",
    "Status.JVM.Memory.NonHeap.Committed",
    "Status.JVM.Memory.NonHeap.Max",
    "Status.JVM.Memory.NonHeap.Used",
    "Status.JVM.Threads.Count",
    "numPendingTaskManagers",
    "numRegisteredTaskManagers",
    "numRunningJobs",
    "startWorkFailurePerSecond",
    "taskSlotsAvailable",
    "taskSlotsTotal"
  )

  def fromRaw(fcid: Fcid, raw: Map[String, String]): FlinkJmMetrics = FlinkJmMetrics(
    clusterId = fcid.clusterId,
    namespace = fcid.namespace,
    jvmClassLoaderClassesLoaded = raw.get("Status.JVM.ClassLoader.ClassesLoaded").map(_.toLong),
    jvmClassLoaderClassesUnloaded = raw.get("Status.JVM.ClassLoader.ClassesUnloaded").map(_.toLong),
    jvmCpuLoad = raw.get("Status.JVM.CPU.Load").map(_.toDouble),
    jvmCpuTime = raw.get("Status.JVM.CPU.Time").map(_.toLong),
    jvmGarbageCollectorCopyCount = raw.get("Status.JVM.GarbageCollector.Copy.Count").map(_.toLong),
    jvmGarbageCollectorCopyTime = raw.get("Status.JVM.GarbageCollector.Copy.Time").map(_.toLong),
    jvmGarbageCollectorMarkSweepCompactCount = raw.get("Status.JVM.GarbageCollector.MarkSweepCompact.Count").map(_.toLong),
    jvmGarbageCollectorMarkSweepCompactTime = raw.get("Status.JVM.GarbageCollector.MarkSweepCompact.Time").map(_.toLong),
    jvmMemoryDirectCount = raw.get("Status.JVM.Memory.Direct.Count").map(_.toLong),
    jvmMemoryDirectMemoryUsed = raw.get("Status.JVM.Memory.Direct.MemoryUsed").map(_.toLong),
    jvmMemoryDirectTotalCapacity = raw.get("Status.JVM.Memory.Direct.TotalCapacity").map(_.toLong),
    jvmMemoryHeapCommitted = raw.get("Status.JVM.Memory.Heap.Committed").map(_.toLong),
    jvmMemoryHeapMax = raw.get("Status.JVM.Memory.Heap.Max").map(_.toLong),
    jvmMemoryHeapUsed = raw.get("Status.JVM.Memory.Heap.Used").map(_.toLong),
    jvmMemoryMappedCount = raw.get("Status.JVM.Memory.Mapped.Count").map(_.toLong),
    jvmMemoryMappedMemoryUsed = raw.get("Status.JVM.Memory.Mapped.MemoryUsed").map(_.toLong),
    jvmMemoryMappedTotalCapacity = raw.get("Status.JVM.Memory.Mapped.TotalCapacity").map(_.toLong),
    jvmMemoryMetaspaceCommitted = raw.get("Status.JVM.Memory.Metaspace.Committed").map(_.toLong),
    jvmMemoryMetaspaceMax = raw.get("Status.JVM.Memory.Metaspace.Max").map(_.toLong),
    jvmMemoryMetaspaceUsed = raw.get("Status.JVM.Memory.Metaspace.Used").map(_.toLong),
    jvmMemoryNonHeapCommitted = raw.get("Status.JVM.Memory.NonHeap.Committed").map(_.toLong),
    jvmMemoryNonHeapMax = raw.get("Status.JVM.Memory.NonHeap.Max").map(_.toLong),
    jvmMemoryNonHeapUsed = raw.get("Status.JVM.Memory.NonHeap.Used").map(_.toLong),
    jvmThreadsCount = raw.get("Status.JVM.Threads.Count").map(_.toLong),
    numPendingTaskManagers = raw.get("numPendingTaskManagers").map(_.toLong),
    numRegisteredTaskManagers = raw.get("numRegisteredTaskManagers").map(_.toLong),
    numRunningJobs = raw.get("numRunningJobs").map(_.toLong),
    startWorkFailurePerSecond = raw.get("startWorkFailurePerSecond").map(_.toDouble),
    taskSlotsAvailable = raw.get("taskSlotsAvailable").map(_.toLong),
    taskSlotsTotal = raw.get("taskSlotsTotal").map(_.toLong)
  )
