package org.apache.dolphinscheduler.dao.repository;

import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelationLog;

import java.util.List;

public interface ProcessTaskRelationLogDao {

    List<ProcessTaskRelationLog> queryProcessTaskRelationLogByWorkflow(long workflowDefinitionCode,
                                                                       int workflowDefinitionVersion);
}
