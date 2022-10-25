package org.apache.dolphinscheduler.api.checker;

import lombok.NonNull;
import org.apache.dolphinscheduler.api.vo.IsolationTaskExcelParseVO;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;

public interface IsolationTaskChecker {

    void checkCanSubmitTaskIsolation(@NonNull User loginUser,
                                     long projectCode,
                                     ProcessInstance processInstance,
                                     @NonNull List<IsolationTaskExcelParseVO> voList);

    void checkCanOnlineTaskIsolation(@NonNull User loginUser,
                                     long projectCode,
                                     @NonNull ProcessInstance processInstance,
                                     @NonNull IsolationTask isolationTask);

    void checkCanListingTaskIsolation(@NonNull User loginUser,
                                      long projectCode);

    void checkCanDeleteTaskIsolation(@NonNull User loginUser,
                                     long projectCode,
                                     long isolationId);

    void checkCanCancelTaskIsolation(@NonNull User loginUser,
                                     long projectCode,
                                     ProcessInstance processInstance,
                                     @NonNull IsolationTask isolationTasks);
}
