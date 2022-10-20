package org.apache.dolphinscheduler.dao.repository.impl;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.repository.ProcessDefinitionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class ProcessDefinitionDaoImpl implements ProcessDefinitionDao {

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Override
    public ProcessDefinition queryProcessDefinitionByCode(@NonNull Long processDefinitionCode) {
        return processDefinitionMapper.queryByCode(processDefinitionCode);
    }
}
