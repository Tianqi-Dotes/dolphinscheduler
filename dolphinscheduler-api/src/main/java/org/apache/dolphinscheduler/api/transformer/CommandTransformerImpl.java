package org.apache.dolphinscheduler.api.transformer;

import lombok.NonNull;
import org.apache.dolphinscheduler.api.checker.WorkflowInstanceChecker;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.repository.ProcessInstanceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.dolphinscheduler.common.Constants.CMD_PARAM_CLEAN_STATE_TASK_INSTANCE_IDS;

@Component
public class CommandTransformerImpl implements CommandTransformer {

    @Autowired
    private ProcessInstanceDao processInstanceDao;

    @Autowired
    private WorkflowInstanceChecker workflowInstanceChecker;

    @Override
    public List<Command> transformToCleanTaskInstanceStateCommands(@NonNull User loginUser,
                                                                   @NonNull Map<Integer, List<Integer>> workflowInstanceId2TaskInstanceIds) {
        List<Command> commands = new ArrayList<>(workflowInstanceId2TaskInstanceIds.size());
        workflowInstanceId2TaskInstanceIds.forEach((workflowInstanceId, taskInstanceIds) -> {
            Command command = transformToCleanTaskInstanceStateCommand(loginUser, workflowInstanceId, taskInstanceIds);
            commands.add(command);
        });
        return commands;
    }

    @Override
    public Command transformToCleanTaskInstanceStateCommand(@NonNull User loginUser,
                                                            @NonNull Integer workflowInstanceId,
                                                            @NonNull List<Integer> needCleanTaskInstances) {
        ProcessInstance processInstance = processInstanceDao.queryProcessInstanceById(workflowInstanceId);
        workflowInstanceChecker.checkCanCleanTaskInstanceState(loginUser, processInstance);

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
        commandParamMap.put(CMD_PARAM_CLEAN_STATE_TASK_INSTANCE_IDS, JSONUtils.toJsonString(needCleanTaskInstances));
        command.setCommandParam(JSONUtils.toJsonString(commandParamMap));
        command.setProcessInstanceId(processInstance.getId());
        return command;
    }
}
