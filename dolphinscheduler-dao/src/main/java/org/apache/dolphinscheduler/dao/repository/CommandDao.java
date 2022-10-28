package org.apache.dolphinscheduler.dao.repository;

import org.apache.dolphinscheduler.dao.entity.Command;

import java.util.List;

public interface CommandDao {

    void batchInsertCommand(List<Command> commands);

    List<Command> queryRecoveryCoronationCommandByWorkflowInstanceId(long workflowInstanceId);
}
