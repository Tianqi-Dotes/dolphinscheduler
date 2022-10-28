package org.apache.dolphinscheduler.api.service;

import lombok.NonNull;
import org.apache.dolphinscheduler.api.dto.request.CoronationTaskListingRequest;
import org.apache.dolphinscheduler.api.dto.request.CoronationTaskParseRequest;
import org.apache.dolphinscheduler.api.dto.request.CoronationTaskSubmitRequest;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.vo.CoronationTaskParseVO;
import org.apache.dolphinscheduler.dao.dto.CoronationTaskDTO;
import org.apache.dolphinscheduler.dao.entity.CoronationTask;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;

public interface CoronationTaskService {

    List<CoronationTaskParseVO> parseCoronationTask(@NonNull User loginUser,
                                                    long projectCode,
                                                    @NonNull CoronationTaskParseRequest request);

    void submitCoronationTask(@NonNull User loginUser,
                              long projectCode,
                              @NonNull CoronationTaskSubmitRequest request);

    PageInfo<CoronationTaskDTO> listingCoronationTasks(@NonNull User loginUser,
                                                       long projectCode,
                                                       @NonNull CoronationTaskListingRequest request);

    void cancelCoronationTask(@NonNull User loginUser, long projectCode, long id);
}
