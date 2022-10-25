package org.apache.dolphinscheduler.api.transformer;

import lombok.NonNull;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.common.Constants.CMD_PARAM_CLEAN_STATE_TASK_INSTANCE_IDS;
import static org.apache.dolphinscheduler.common.Constants.CMD_PARAM_RECOVERY_KILLED_ISOLATED_TASK_IDS;
import static org.apache.dolphinscheduler.common.Constants.CMD_PARAM_RECOVERY_PAUSED_ISOLATED_TASK_IDS;

@Component
public class CommandTransformerImpl implements CommandTransformer {

    @Override
    public Command transformToCleanTaskInstanceStateCommand(@NonNull ProcessInstance processInstance,
                                                            @NonNull List<Integer> needCleanTaskInstanceIds) {

        Command command = new Command();
        command.setCommandType(CommandType.START_FROM_STATE_CLEAN_TASKS);
        command.setExecutorId(processInstance.getExecutorId());
        command.setProcessDefinitionCode(processInstance.getProcessDefinitionCode());
        command.setProcessDefinitionVersion(processInstance.getProcessDefinitionVersion());
        // we set the task post strategy, since we will go through the post task if the post task is not running
        command.setTaskDependType(TaskDependType.TASK_POST);
        command.setFailureStrategy(processInstance.getFailureStrategy());
        command.setWarningType(processInstance.getWarningType());
        command.setWarningGroupId(processInstance.getWarningGroupId());
        command.setProcessInstancePriority(processInstance.getProcessInstancePriority());
        command.setWorkerGroup(processInstance.getWorkerGroup());
        command.setEnvironmentCode(processInstance.getEnvironmentCode());
        command.setDryRun(processInstance.getDryRun());

        // todo：use pojo to represent CommandParam rather than map
        Map<String, String> commandParamMap = new HashMap<>();
        commandParamMap.put(CMD_PARAM_CLEAN_STATE_TASK_INSTANCE_IDS, JSONUtils.toJsonString(needCleanTaskInstanceIds));
        command.setCommandParam(JSONUtils.toJsonString(commandParamMap));
        command.setProcessInstanceId(processInstance.getId());
        return command;
    }

    @Override
    public Command transformToRecoveryFromTaskIsolationCommand(@NonNull ProcessInstance processInstance,
                                                               @NonNull List<TaskInstance> canRecoveryIsolationTaskInstances) {
        Command command = new Command();
        command.setCommandType(CommandType.RECOVERY_FROM_ISOLATION_TASKS);

        command.setExecutorId(processInstance.getExecutorId());
        command.setProcessDefinitionCode(processInstance.getProcessDefinitionCode());
        command.setProcessDefinitionVersion(processInstance.getProcessDefinitionVersion());
        // we set the task post strategy, since we will go through the post task if the post task is not running
        command.setTaskDependType(TaskDependType.TASK_POST);
        command.setFailureStrategy(processInstance.getFailureStrategy());
        command.setWarningType(processInstance.getWarningType());
        command.setWarningGroupId(processInstance.getWarningGroupId());
        command.setProcessInstancePriority(processInstance.getProcessInstancePriority());
        command.setWorkerGroup(processInstance.getWorkerGroup());
        command.setEnvironmentCode(processInstance.getEnvironmentCode());
        command.setDryRun(processInstance.getDryRun());

        // todo：use pojo to represent CommandParam rather than map
        List<Integer> recoveryPausedIsolationIds = canRecoveryIsolationTaskInstances.stream()
                .filter(taskInstance -> taskInstance.getState().typeIsPauseByIsolation())
                .map(TaskInstance::getId)
                .collect(Collectors.toList());
        List<Integer> recoveryKilledIsolationIds = canRecoveryIsolationTaskInstances.stream()
                .filter(taskInstance -> taskInstance.getState().typeIsKilledByIsolation())
                .map(TaskInstance::getId)
                .collect(Collectors.toList());
        Map<String, String> commandParamMap = new HashMap<>();
        commandParamMap.put(CMD_PARAM_RECOVERY_PAUSED_ISOLATED_TASK_IDS,
                JSONUtils.toJsonString(recoveryPausedIsolationIds));
        commandParamMap.put(CMD_PARAM_RECOVERY_KILLED_ISOLATED_TASK_IDS,
                JSONUtils.toJsonString(recoveryKilledIsolationIds));
        command.setCommandParam(JSONUtils.toJsonString(commandParamMap));
        command.setProcessInstanceId(processInstance.getId());
        return command;
    }

}
