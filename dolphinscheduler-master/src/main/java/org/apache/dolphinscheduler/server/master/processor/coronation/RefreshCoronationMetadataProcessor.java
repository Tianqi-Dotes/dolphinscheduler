package org.apache.dolphinscheduler.server.master.processor.coronation;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.server.master.service.CoronationMetadataManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RefreshCoronationMetadataProcessor implements NettyRequestProcessor {

    @Autowired
    private CoronationMetadataManager coronationMetadataManager;

    @Override
    public void process(Channel channel, Command command) {
        if (command.getType() != CommandType.REFRESH_CORONATION_METADATA_REQUEST) {
            throw new IllegalArgumentException(String.format("The current rpc command : %s is invalidated", command));
        }
        coronationMetadataManager.refreshCoronationTaskMetadata();
    }
}
