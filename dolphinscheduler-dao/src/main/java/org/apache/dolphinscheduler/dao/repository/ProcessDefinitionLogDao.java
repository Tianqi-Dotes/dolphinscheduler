package org.apache.dolphinscheduler.dao.repository;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionLog;

import java.util.Optional;

public interface ProcessDefinitionLogDao {

    Optional<ProcessDefinitionLog> queryProcessDefinitionByCode(@NonNull Long processDefinitionCode,
                                                                @NonNull Integer processDefinitionVersion);
}
