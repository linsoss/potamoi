package potamoi.flink.interp

trait SqlInteropExecutor {

  def execute(sql: String): Unit

}
