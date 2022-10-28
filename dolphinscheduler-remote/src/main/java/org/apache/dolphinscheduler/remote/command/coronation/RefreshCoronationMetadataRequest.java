package org.apache.dolphinscheduler.remote.command.coronation;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;

import java.io.Serializable;

public class RefreshCoronationMetadataRequest implements Serializable {

    public Command convert2Command() {
        Command command = new Command();
        command.setType(CommandType.REFRESH_CORONATION_METADATA_REQUEST);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }

}
