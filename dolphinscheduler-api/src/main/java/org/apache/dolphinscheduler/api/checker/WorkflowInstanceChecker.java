package org.apache.dolphinscheduler.api.checker;

import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.User;

public interface WorkflowInstanceChecker {

    void checkCanCleanTaskInstanceState(User loginUser, ProcessInstance processInstance) throws ServiceException;

}
