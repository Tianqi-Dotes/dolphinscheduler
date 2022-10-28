package org.apache.dolphinscheduler.dao.repository;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;

import java.util.Optional;

public interface ProcessDefinitionDao {

    Optional<ProcessDefinition> queryProcessDefinitionByCode(@NonNull Long processDefinitionCode);
}
