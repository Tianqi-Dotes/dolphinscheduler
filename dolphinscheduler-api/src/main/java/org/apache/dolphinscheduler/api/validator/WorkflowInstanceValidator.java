package org.apache.dolphinscheduler.api.validator;

import lombok.NonNull;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class WorkflowInstanceValidator {

    public static void validateWorkflowInstanceAllExist(@NonNull List<Integer> workflowInstanceIds,
                                                        @NonNull List<ProcessInstance> existWorkflowInstances,
                                                        @NonNull ServiceException serviceException) {
        Set<Integer> existWorkflowInstanceIds =
                existWorkflowInstances.stream().map(ProcessInstance::getId).collect(Collectors.toSet());
        for (Integer workflowInstanceId : workflowInstanceIds) {
            if (!existWorkflowInstanceIds.contains(workflowInstanceId)) {
                throw serviceException;
            }
        }
    }

    public static void validateWorkflowCanExecuteCleanTaskInstanceState(@NonNull List<ProcessInstance> processInstances,
                                                                        ServiceException serviceException) {

    }
}
