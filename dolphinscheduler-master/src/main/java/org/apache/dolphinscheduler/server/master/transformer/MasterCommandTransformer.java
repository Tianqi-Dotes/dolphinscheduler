package org.apache.dolphinscheduler.server.master.transformer;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;

import java.util.List;

public interface MasterCommandTransformer {

    Command transformToRecoveryFromCoronationPauseCommand(@NonNull ProcessInstance processInstance,
                                                          @NonNull List<Integer> needToRecoveryTaskInstanceIds);

}
