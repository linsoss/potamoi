package potamoi.flink.interp

import potamoi.common.Err

abstract class FlinkInterpErr(msg: String, cause: Throwable = null) extends Err(msg, cause)
