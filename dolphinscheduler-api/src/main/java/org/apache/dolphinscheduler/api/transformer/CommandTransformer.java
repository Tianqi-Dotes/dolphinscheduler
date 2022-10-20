package org.apache.dolphinscheduler.api.transformer;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;
import java.util.Map;

public interface CommandTransformer {

    List<Command> transformToCleanTaskInstanceStateCommands(@NonNull User loginUser,
                                                            @NonNull Map<Integer, List<Integer>> workflowInstanceId2TaskInstanceIds);

    Command transformToCleanTaskInstanceStateCommand(@NonNull User loginUser,
                                                     @NonNull Integer workflowInstanceId,
                                                     @NonNull List<Integer> needCleanTaskInstances);
}
