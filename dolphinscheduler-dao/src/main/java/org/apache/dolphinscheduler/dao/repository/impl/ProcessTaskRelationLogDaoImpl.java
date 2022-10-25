package org.apache.dolphinscheduler.dao.repository.impl;

import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelationLog;
import org.apache.dolphinscheduler.dao.mapper.ProcessTaskRelationLogMapper;
import org.apache.dolphinscheduler.dao.repository.ProcessTaskRelationLogDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class ProcessTaskRelationLogDaoImpl implements ProcessTaskRelationLogDao {

    @Autowired
    private ProcessTaskRelationLogMapper processTaskRelationLogMapper;

    @Override
    public List<ProcessTaskRelationLog> queryProcessTaskRelationLogByWorkflow(
                                                                              long workflowDefinitionCode,
                                                                              int workflowDefinitionVersion) {
        return processTaskRelationLogMapper.queryByProcessCodeAndVersion(workflowDefinitionCode,
                workflowDefinitionVersion);
    }
}
