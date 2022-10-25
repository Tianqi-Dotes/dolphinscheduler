package org.apache.dolphinscheduler.server.master.utils;

import lombok.NonNull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Environment;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.plugin.task.api.enums.Direct;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.dolphinscheduler.common.Constants.DEFAULT_WORKER_GROUP;

public class TaskInstanceUtils {

    public static TaskInstance newTaskInstance(@NonNull ProcessInstance processInstance,
                                               @NonNull TaskNode taskNode) {
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setTaskCode(taskNode.getCode());
        taskInstance.setTaskDefinitionVersion(taskNode.getVersion());
        // task name
        taskInstance.setName(taskNode.getName());
        // task instance state
        taskInstance.setState(ExecutionStatus.SUBMITTED_SUCCESS);
        // process instance id
        taskInstance.setProcessInstanceId(processInstance.getId());
        // task instance type
        taskInstance.setTaskType(taskNode.getType().toUpperCase());
        // task instance whether alert
        taskInstance.setAlertFlag(Flag.NO);

        // task instance start time
        taskInstance.setStartTime(null);

        // task instance flag
        taskInstance.setFlag(Flag.YES);

        // task dry run flag
        taskInstance.setDryRun(processInstance.getDryRun());

        // task instance retry times
        taskInstance.setRetryTimes(0);

        // max task instance retry times
        taskInstance.setMaxRetryTimes(taskNode.getMaxRetryTimes());

        // retry task instance interval
        taskInstance.setRetryInterval(taskNode.getRetryInterval());

        // set task param
        taskInstance.setTaskParams(taskNode.getTaskParams());

        // set task group and priority
        taskInstance.setTaskGroupId(taskNode.getTaskGroupId());
        taskInstance.setTaskGroupPriority(taskNode.getTaskGroupPriority());

        // task instance priority
        if (taskNode.getTaskInstancePriority() == null) {
            taskInstance.setTaskInstancePriority(Priority.MEDIUM);
        } else {
            taskInstance.setTaskInstancePriority(taskNode.getTaskInstancePriority());
        }

        String processWorkerGroup = processInstance.getWorkerGroup();
        processWorkerGroup = StringUtils.isBlank(processWorkerGroup) ? DEFAULT_WORKER_GROUP : processWorkerGroup;
        String taskWorkerGroup =
                StringUtils.isBlank(taskNode.getWorkerGroup()) ? processWorkerGroup : taskNode.getWorkerGroup();

        Long processEnvironmentCode =
                Objects.isNull(processInstance.getEnvironmentCode()) ? -1 : processInstance.getEnvironmentCode();
        Long taskEnvironmentCode =
                Objects.isNull(taskNode.getEnvironmentCode()) ? processEnvironmentCode : taskNode.getEnvironmentCode();

        if (!processWorkerGroup.equals(DEFAULT_WORKER_GROUP) && taskWorkerGroup.equals(DEFAULT_WORKER_GROUP)) {
            taskInstance.setWorkerGroup(processWorkerGroup);
            taskInstance.setEnvironmentCode(processEnvironmentCode);
        } else {
            taskInstance.setWorkerGroup(taskWorkerGroup);
            taskInstance.setEnvironmentCode(taskEnvironmentCode);
        }

        // delay execution time
        taskInstance.setDelayTime(taskNode.getDelayTime());
        return taskInstance;
    }

    public static TaskInstance cloneTaskInstance(@NonNull ProcessInstance processInstance,
                                                 @NonNull TaskNode taskNode,
                                                 TaskInstance oldTaskInstance) {
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setTaskCode(taskNode.getCode());
        taskInstance.setTaskDefinitionVersion(taskNode.getVersion());
        // task name
        taskInstance.setName(taskNode.getName());
        // task instance state
        taskInstance.setState(ExecutionStatus.SUBMITTED_SUCCESS);
        // process instance id
        taskInstance.setProcessInstanceId(processInstance.getId());
        // task instance type
        taskInstance.setTaskType(taskNode.getType().toUpperCase());
        // task instance whether alert
        taskInstance.setAlertFlag(Flag.NO);

        // task instance start time
        taskInstance.setStartTime(null);

        // task instance flag
        taskInstance.setFlag(Flag.YES);

        // task dry run flag
        taskInstance.setDryRun(processInstance.getDryRun());

        // task instance retry times
        taskInstance.setRetryTimes(0);

        // max task instance retry times
        taskInstance.setMaxRetryTimes(taskNode.getMaxRetryTimes());

        // retry task instance interval
        taskInstance.setRetryInterval(taskNode.getRetryInterval());

        // set task param
        taskInstance.setTaskParams(taskNode.getTaskParams());

        // set task group and priority
        taskInstance.setTaskGroupId(taskNode.getTaskGroupId());
        taskInstance.setTaskGroupPriority(taskNode.getTaskGroupPriority());

        // task instance priority
        if (taskNode.getTaskInstancePriority() == null) {
            taskInstance.setTaskInstancePriority(Priority.MEDIUM);
        } else {
            taskInstance.setTaskInstancePriority(taskNode.getTaskInstancePriority());
        }

        String processWorkerGroup = processInstance.getWorkerGroup();
        processWorkerGroup = StringUtils.isBlank(processWorkerGroup) ? DEFAULT_WORKER_GROUP : processWorkerGroup;
        String taskWorkerGroup =
                StringUtils.isBlank(taskNode.getWorkerGroup()) ? processWorkerGroup : taskNode.getWorkerGroup();

        Long processEnvironmentCode =
                Objects.isNull(processInstance.getEnvironmentCode()) ? -1 : processInstance.getEnvironmentCode();
        Long taskEnvironmentCode =
                Objects.isNull(taskNode.getEnvironmentCode()) ? processEnvironmentCode : taskNode.getEnvironmentCode();

        if (!processWorkerGroup.equals(DEFAULT_WORKER_GROUP) && taskWorkerGroup.equals(DEFAULT_WORKER_GROUP)) {
            taskInstance.setWorkerGroup(processWorkerGroup);
            taskInstance.setEnvironmentCode(processEnvironmentCode);
        } else {
            taskInstance.setWorkerGroup(taskWorkerGroup);
            taskInstance.setEnvironmentCode(taskEnvironmentCode);
        }

        taskInstance.setDelayTime(taskNode.getDelayTime());
        taskInstance.setTaskDefine(oldTaskInstance.getTaskDefine());
        taskInstance.setProcessDefine(oldTaskInstance.getProcessDefine());
        taskInstance.setProcessInstance(processInstance);
        return taskInstance;
    }

    public static void injectEnvironment(@NonNull TaskInstance taskInstance, Environment environment) {
        if (Objects.nonNull(environment) && StringUtils.isNotEmpty(environment.getConfig())) {
            taskInstance.setEnvironmentConfig(environment.getConfig());
        }
    }

    public static void injectTaskVarPoolFromPreTask(@NonNull TaskInstance taskInstance,
                                                    @NonNull ProcessInstance processInstance,
                                                    Set<String> preTaskCodes,
                                                    @NonNull Map<Long, Integer> completeTaskMap,
                                                    @NonNull Map<Integer, TaskInstance> taskInstanceMap) {
        if (CollectionUtils.isEmpty(preTaskCodes)) {
            if (StringUtils.isNotEmpty(processInstance.getVarPool())) {
                taskInstance.setVarPool(processInstance.getVarPool());
            }
            return;
        }

        Map<String, Property> allProperty = new HashMap<>();
        Map<String, TaskInstance> allTaskInstance = new HashMap<>();
        for (String preTaskCode : preTaskCodes) {
            Integer taskId = completeTaskMap.get(Long.parseLong(preTaskCode));
            if (taskId == null) {
                continue;
            }
            TaskInstance preTaskInstance = taskInstanceMap.get(taskId);
            if (preTaskInstance == null) {
                continue;
            }
            String preVarPool = preTaskInstance.getVarPool();
            if (StringUtils.isNotEmpty(preVarPool)) {
                List<Property> properties = JSONUtils.toList(preVarPool, Property.class);
                for (Property info : properties) {
                    setVarPoolValue(allProperty, allTaskInstance, preTaskInstance, info);
                }
            }
        }
        if (allProperty.size() > 0) {
            taskInstance.setVarPool(JSONUtils.toJsonString(allProperty.values()));
        }
    }

    private static void setVarPoolValue(Map<String, Property> allProperty,
                                        Map<String, TaskInstance> allTaskInstance,
                                        TaskInstance preTaskInstance,
                                        Property thisProperty) {
        // for this taskInstance all the param in this part is IN.
        thisProperty.setDirect(Direct.IN);
        // get the pre taskInstance Property's name
        String proName = thisProperty.getProp();
        // if the Previous nodes have the Property of same name
        if (allProperty.containsKey(proName)) {
            // comparison the value of two Property
            Property otherPro = allProperty.get(proName);
            // if this property'value of loop is empty,use the other,whether the other's value is empty or not
            if (StringUtils.isEmpty(thisProperty.getValue())) {
                allProperty.put(proName, otherPro);
                // if property'value of loop is not empty,and the other's value is not empty too, use the earlier value
            } else if (StringUtils.isNotEmpty(otherPro.getValue())) {
                TaskInstance otherTask = allTaskInstance.get(proName);
                if (otherTask.getEndTime().getTime() > preTaskInstance.getEndTime().getTime()) {
                    allProperty.put(proName, thisProperty);
                    allTaskInstance.put(proName, preTaskInstance);
                } else {
                    allProperty.put(proName, otherPro);
                }
            } else {
                allProperty.put(proName, thisProperty);
                allTaskInstance.put(proName, preTaskInstance);
            }
        } else {
            allProperty.put(proName, thisProperty);
            allTaskInstance.put(proName, preTaskInstance);
        }
    }
}
