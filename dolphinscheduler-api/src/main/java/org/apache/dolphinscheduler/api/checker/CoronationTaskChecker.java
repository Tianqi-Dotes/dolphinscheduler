package org.apache.dolphinscheduler.api.checker;

import lombok.NonNull;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.vo.CoronationTaskExcelImportVO;
import org.apache.dolphinscheduler.api.vo.CoronationTaskParseVO;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.dao.entity.CoronationTask;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;

public interface CoronationTaskChecker {

    void checkCanParseCoronationTask(@NonNull User loginUser,
                                     @NonNull Project project,
                                     ProcessInstance processInstance,
                                     @NonNull CoronationTaskExcelImportVO vo) throws ServiceException;

    void checkCanSubmitTaskCoronation(@NonNull User loginUser,
                                      @NonNull Project project,
                                      @NonNull ProcessInstance processInstance,
                                      @NonNull DAG<String, TaskNode, TaskNodeRelation> workflowDAG,
                                      @NonNull List<CoronationTaskParseVO> voList);

    void checkCanListingTaskCoronation(@NonNull User loginUser,
                                       long projectCode);

    void checkCanCancelTaskCoronation(@NonNull User loginUser,
                                      long projectCode,
                                      @NonNull ProcessInstance processInstance,
                                      @NonNull CoronationTask coronationTask);

}
