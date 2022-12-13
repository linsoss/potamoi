package potamoi.flink.model

import potamoi.curTs
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink task manager details.
 */
case class FlinkTmDetail(
    id: String,
    path: String,
    dataPort: Int,
    slotsNumber: Int,
    freeSlots: Int,
    totalResource: TmResource,
    freeResource: TmResource,
    hardware: TmHardware,
    memoryConfiguration: TmMemoryConfig,
    ts: Long = curTs)

case class TmResource(
    cpuCores: Float,
    taskHeapMemory: Long,
    taskOffHeapMemory: Long,
    managedMemory: Long,
    networkMemory: Long)

case class TmHardware(
    cpuCores: Float,
    physicalMemory: Long,
    freeMemory: Long,
    managedMemory: Long)

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

object FlinkTmDetail:
  given JsonCodec[TmResource]     = DeriveJsonCodec.gen[TmResource]
  given JsonCodec[TmHardware]     = DeriveJsonCodec.gen[TmHardware]
  given JsonCodec[TmMemoryConfig] = DeriveJsonCodec.gen[TmMemoryConfig]
  given JsonCodec[FlinkTmDetail]  = DeriveJsonCodec.gen[FlinkTmDetail]
  given Ordering[FlinkTmDetail]   = Ordering.by(_.id)
