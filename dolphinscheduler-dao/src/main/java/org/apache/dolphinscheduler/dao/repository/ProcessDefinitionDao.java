package org.apache.dolphinscheduler.dao.repository;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;

public interface ProcessDefinitionDao {

    ProcessDefinition queryProcessDefinitionByCode(@NonNull Long processDefinitionCode);
}
