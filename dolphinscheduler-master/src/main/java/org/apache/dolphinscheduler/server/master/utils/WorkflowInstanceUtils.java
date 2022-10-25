package org.apache.dolphinscheduler.server.master.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.common.Constants.CMD_PARAM_RECOVERY_START_NODE_STRING;
import static org.apache.dolphinscheduler.common.Constants.CMD_PARAM_START_NODES;
import static org.apache.dolphinscheduler.common.Constants.COMMA;

@Slf4j
public class WorkflowInstanceUtils {

    public static List<Integer> getStartTaskInstanceIds(@NonNull ProcessInstance processInstance) {
        switch (processInstance.getCommandType()) {
            case START_FAILURE_TASK_PROCESS:
                return getStartTaskInstanceIdsFromRecoverParam(processInstance);
            case RECOVER_SUSPENDED_PROCESS:
                return getStartTaskInstanceIdsFromRecoverParam(processInstance);
            case START_FROM_STATE_CLEAN_TASKS:
                return getStartTaskInstanceIdsFromStateCleanParam(processInstance);
            case RECOVERY_FROM_ISOLATION_TASKS:
                return getStartTaskInstanceIdsFromRecoverIsolationParam(processInstance);
            default:
                return Collections.emptyList();
        }
    }

    public static List<Integer> getStartTaskInstanceIdsFromRecoverParam(@NonNull ProcessInstance processInstance) {
        Map<String, String> paramMap = JSONUtils.toMap(processInstance.getCommandParam());

        if (paramMap != null && paramMap.containsKey(CMD_PARAM_RECOVERY_START_NODE_STRING)) {
            return Arrays.stream(paramMap.get(CMD_PARAM_RECOVERY_START_NODE_STRING).split(COMMA))
                    .filter(StringUtils::isNotEmpty)
                    .map(Integer::valueOf)
                    .collect(Collectors.toList());

        }
        return Collections.emptyList();
    }

    public static List<Integer> getStartTaskInstanceIdsFromStateCleanParam(@NonNull ProcessInstance processInstance) {
        Map<String, String> commandParamMap = JSONUtils.toMap(processInstance.getCommandParam());
        if (MapUtils.isEmpty(commandParamMap)) {
            return Collections.emptyList();
        }
        List<Integer> stateCleanTaskInstanceIds =
                JSONUtils.parseObject(commandParamMap.get(Constants.CMD_PARAM_CLEAN_STATE_TASK_INSTANCE_IDS),
                        new TypeReference<ArrayList<Integer>>() {
                        });
        if (stateCleanTaskInstanceIds == null) {
            return Collections.emptyList();
        }
        return stateCleanTaskInstanceIds;
    }

    public static List<Integer> getStartTaskInstanceIdsFromRecoverIsolationParam(@NonNull ProcessInstance processInstance) {
        Map<String, String> commandParamMap = JSONUtils.toMap(processInstance.getCommandParam());
        List<Integer> recoveryPausedIsolationIds =
                JSONUtils.parseObject(commandParamMap.get(Constants.CMD_PARAM_RECOVERY_PAUSED_ISOLATED_TASK_IDS),
                        new TypeReference<ArrayList<Integer>>() {
                        });
        List<Integer> recoveryKilledIsolationIds =
                JSONUtils.parseObject(commandParamMap.get(Constants.CMD_PARAM_RECOVERY_KILLED_ISOLATED_TASK_IDS),
                        new TypeReference<ArrayList<Integer>>() {
                        });
        List<Integer> result = new ArrayList<>();
        result.addAll(recoveryPausedIsolationIds);
        result.addAll(recoveryKilledIsolationIds);
        return result;
    }

    public static List<String> getStartNodeName(@NonNull ProcessInstance processInstance) {
        List<String> startNodeNameList = new ArrayList<>();
        Map<String, String> paramMap = JSONUtils.toMap(processInstance.getCommandParam());
        if (paramMap == null) {
            return startNodeNameList;
        }
        if (paramMap.containsKey(CMD_PARAM_START_NODES)) {
            startNodeNameList = Arrays.asList(paramMap.get(CMD_PARAM_START_NODES).split(Constants.COMMA));
        }
        return startNodeNameList;
    }

    public static void injectWorkflowVarPoolFromEndTaskInstance(@NonNull ProcessInstance processInstance,
                                                                @NonNull TaskInstance endTaskInstance) {
        String taskInstanceVarPool = endTaskInstance.getVarPool();
        if (StringUtils.isNotEmpty(taskInstanceVarPool)) {
            Set<Property> taskProperties = new HashSet<>(JSONUtils.toList(taskInstanceVarPool, Property.class));
            String processInstanceVarPool = processInstance.getVarPool();
            if (StringUtils.isNotEmpty(processInstanceVarPool)) {
                Set<Property> properties = new HashSet<>(JSONUtils.toList(processInstanceVarPool, Property.class));
                properties.addAll(taskProperties);
                processInstance.setVarPool(JSONUtils.toJsonString(properties));
            } else {
                processInstance.setVarPool(JSONUtils.toJsonString(taskProperties));
            }
        }
    }

    /**
     * Generate complete task instance map, taskCode -> TaskInstance.
     *
     * @param completeTaskMap complete taskCode -> taskId
     * @param taskInstanceMap taskId -> taskInstance
     * @return
     */
    public static Map<String, TaskInstance> getCompleteTaskInstanceMap(@NonNull Map<Long, Integer> completeTaskMap,
                                                                       @NonNull Map<Integer, TaskInstance> taskInstanceMap) {
        Map<String, TaskInstance> completeTaskInstanceMap = new HashMap<>();
        for (Map.Entry<Long, Integer> entry : completeTaskMap.entrySet()) {
            Long taskConde = entry.getKey();
            Integer taskInstanceId = entry.getValue();
            TaskInstance taskInstance = taskInstanceMap.get(taskInstanceId);
            if (taskInstance == null) {
                log.warn("Cannot find the taskInstance from taskInstanceMap, taskInstanceId: {}, taskConde: {}",
                        taskInstanceId,
                        taskConde);
                // This case will happen when we submit to db failed, then the taskInstanceId is 0
                continue;
            }
            completeTaskInstanceMap.put(Long.toString(taskInstance.getTaskCode()), taskInstance);

        }
        return completeTaskInstanceMap;
    }
}
