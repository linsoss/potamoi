package com.github.potamois.potamoi.flinkgateway

/**
 * @author Al-assad
 */
trait InteractServiceTrait {

  def executeStatements(sqlStatements: String, config: ExecConfig = ExecConfig.localEnv()): ExecResult

  def retrieveModifyResult(trackOpsId: String): ModifyResult

  def retrieveChangingQueryResult(trackOpsId: String, lastTs: Option[Long] = None): QueryResult

  def retrievePageableQueryResult(trackOpsId: String, page: PageReq): QueryResultPageSnapshot

}




