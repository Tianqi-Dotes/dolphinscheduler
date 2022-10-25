package org.apache.dolphinscheduler.api.transformer;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

import java.util.List;

public interface CommandTransformer {

    Command transformToCleanTaskInstanceStateCommand(@NonNull ProcessInstance processInstance,
                                                     @NonNull List<Integer> needCleanTaskInstanceIds);

    Command transformToRecoveryFromTaskIsolationCommand(@NonNull ProcessInstance processInstance,
                                                        @NonNull List<TaskInstance> canRecoveryTaskInstances);
}
