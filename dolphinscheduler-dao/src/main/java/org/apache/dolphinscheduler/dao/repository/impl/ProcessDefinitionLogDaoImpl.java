package org.apache.dolphinscheduler.dao.repository.impl;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionLog;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionLogMapper;
import org.apache.dolphinscheduler.dao.repository.ProcessDefinitionLogDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class ProcessDefinitionLogDaoImpl implements ProcessDefinitionLogDao {

    @Autowired
    private ProcessDefinitionLogMapper processDefinitionLogMapper;

    @Override
    public Optional<ProcessDefinitionLog> queryProcessDefinitionByCode(@NonNull Long processDefinitionCode,
                                                                       @NonNull Integer processDefinitionVersion) {
        return Optional.ofNullable(
                processDefinitionLogMapper.queryByDefinitionCodeAndVersion(processDefinitionCode,
                        processDefinitionVersion));
    }
}
