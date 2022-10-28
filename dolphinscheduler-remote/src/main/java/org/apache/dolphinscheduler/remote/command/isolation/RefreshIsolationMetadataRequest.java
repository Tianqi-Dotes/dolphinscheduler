package org.apache.dolphinscheduler.remote.command.isolation;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class RefreshIsolationMetadataRequest implements Serializable {

    public Command convert2Command() {
        Command command = new Command();
        command.setType(CommandType.REFRESH_ISOLATION_METADATA_REQUEST);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }
}
