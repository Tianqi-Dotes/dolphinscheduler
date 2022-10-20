package org.apache.dolphinscheduler.dao.repository.impl;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.mapper.CommandMapper;
import org.apache.dolphinscheduler.dao.repository.CommandDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class CommandDaoImpl implements CommandDao {

    @Autowired
    private CommandMapper commandMapper;

    @Override
    public void batchInsertCommand(List<Command> commands) {
        if (CollectionUtils.isEmpty(commands)) {
            return;
        }
        commandMapper.batchInsertCommand(commands);
    }
}
