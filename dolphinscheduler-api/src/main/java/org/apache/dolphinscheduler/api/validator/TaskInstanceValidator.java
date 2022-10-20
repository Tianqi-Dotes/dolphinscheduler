package org.apache.dolphinscheduler.api.validator;

import lombok.NonNull;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.api.enums.Status.TASK_INSTANCE_NOT_EXISTS;

public class TaskInstanceValidator {

    public static void validateTaskInstanceAllExists(@NonNull List<Integer> taskInstanceIds,
                                                     @NonNull List<TaskInstance> existTaskInstances) throws ServiceException {
        Set<Integer> existTaskInstanceIds =
                existTaskInstances.stream().map(TaskInstance::getId).collect(Collectors.toSet());
        for (Integer taskInstanceId : taskInstanceIds) {
            if (!existTaskInstanceIds.contains(taskInstanceId)) {
                throw new ServiceException(TASK_INSTANCE_NOT_EXISTS, taskInstanceId);
            }
        }
    }
}
