package org.apache.dolphinscheduler.api.service;

import lombok.NonNull;
import org.apache.dolphinscheduler.api.dto.request.IsolationTaskCancelRequest;
import org.apache.dolphinscheduler.api.dto.request.IsolationTaskListingRequest;
import org.apache.dolphinscheduler.api.dto.request.IsolationTaskSubmitRequest;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.vo.IsolationTaskListingVO;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.dolphinscheduler.dao.entity.User;

public interface IsolationTaskService {

    void submitTaskIsolations(@NonNull User loginUser,
                              long projectCode,
                              @NonNull IsolationTaskSubmitRequest isolationTaskSubmitRequest);

    void cancelTaskIsolation(@NonNull User loginUser,
                             long projectCode,
                             long isolationId);

    PageInfo<IsolationTaskListingVO> listingTaskIsolation(@NonNull User loginUser,
                                                          long projectCode,
                                                          @NonNull IsolationTaskListingRequest request);

}
