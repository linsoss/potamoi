package potamoi.flink.model

import potamoi.{curTs, KryoSerializable}
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink task manager details.
 */
case class FlinkTmDetail(
    clusterId: String,
    namespace: String,
    tmId: String,
    path: String,
    dataPort: Int,
    slotsNumber: Int,
    freeSlots: Int,
    totalResource: TmResource,
    freeResource: TmResource,
    hardware: TmHardware,
    memoryConfiguration: TmMemoryConfig,
    ts: Long = curTs)
    extends KryoSerializable
    derives JsonCodec:
  lazy val ftid: Ftid = Ftid(clusterId, namespace, tmId)

case class TmResource(
    cpuCores: Float,
    taskHeapMemory: Long,
    taskOffHeapMemory: Long,
    managedMemory: Long,
    networkMemory: Long)
    derives JsonCodec

case class TmHardware(
    cpuCores: Float,
    physicalMemory: Long,
    freeMemory: Long,
    managedMemory: Long)
    derives JsonCodec

case class TmMemoryConfig(
    frameworkHeap: Long,
    taskHeap: Long,
    frameworkOffHeap: Long,
    taskOffHeap: Long,
    networkMemory: Long,
    managedMemory: Long,
    jvmMetaspace: Long,
    jvmOverhead: Long,
    totalFlinkMemory: Long,
    totalProcessMemory: Long)
    derives JsonCodec
