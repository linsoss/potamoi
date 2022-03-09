package com.github.potamois.potamoi.flinkgateway

import com.github.potamois.potamoi.gateway.flink.ExecConfig

object InteractService extends InteractServiceTrait {

  override def executeStatements(sqlStatements: String, config: ExecConfig): ExecResult = {
    null
  }

  override def retrieveModifyResult(trackOpsId: String): ModifyResult = ???

  override def retrieveChangingQueryResult(trackOpsId: String, lastTs: Option[Long]): QueryResult = ???

  override def retrievePageableQueryResult(trackOpsId: String, page: PageReq): QueryResultPageSnapshot = ???

}
