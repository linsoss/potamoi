package potamoi

import potamoi.flink.interpreter.FlinkInterpBootstrap
import potamoi.flink.FlinkMajorVer

/**
 * Flink 1.16 sql interpreter app.
 */
object FlinkInterpreterAppV116 extends FlinkInterpBootstrap(FlinkMajorVer.V116)
