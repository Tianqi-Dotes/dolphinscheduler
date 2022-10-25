package org.apache.dolphinscheduler.server.master.processor.isolation;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.server.master.service.IsolationTaskManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RefreshIsolationTaskProcessor implements NettyRequestProcessor {

    @Autowired
    private IsolationTaskManager isolationTaskManager;

    @Override
    public void process(Channel channel, Command command) {
        if (command.getType() != CommandType.REFRESH_ISOLATION_REQUEST) {
            throw new IllegalArgumentException(String.format("The current rpc command: %s is invalidated", command));
        }
        isolationTaskManager.refreshIsolationTaskMapFromDB();
    }
}
