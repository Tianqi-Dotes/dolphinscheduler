package org.apache.dolphinscheduler.server.master.transformer;

import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class MasterCommandTransformerImpl implements MasterCommandTransformer {

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Override
    public Command transformToRecoveryFromCoronationPauseCommand(@NonNull ProcessInstance processInstance,
                                                                 @NonNull List<Integer> needToRecoveryTaskInstanceIds) {
        if (CollectionUtils.isEmpty(needToRecoveryTaskInstanceIds)) {
            throw new IllegalArgumentException("The recovery task instance cannot be empty");
        }

        Command command = new Command();
        command.setProcessInstanceId(processInstance.getId());
        command.setProcessDefinitionCode(processInstance.getProcessDefinitionCode());
        command.setProcessDefinitionVersion(processInstance.getProcessDefinitionVersion());
        command.setTaskDependType(processInstance.getTaskDependType());
        command.setFailureStrategy(processInstance.getFailureStrategy());
        command.setCommandType(CommandType.RECOVERY_FROM_CORONATION_PAUSE_TASKS);

        Map<String, String> map = new HashMap<>();
        map.put(Constants.CMD_PARAM_RECOVERY_PAUSED_BY_CORONATION_TASK_IDS,
                JSONUtils.toJsonString(needToRecoveryTaskInstanceIds));
        command.setCommandParam(JSONUtils.toJsonString(map));
        return command;
    }
}
