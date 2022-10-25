package org.apache.dolphinscheduler.api.remote;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.remote.NettyRemotingClient;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.config.NettyClientConfig;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ApiServerRPCClient {

    private final NettyRemotingClient client;

    private static final long DEFAULT_TIME_OUT_MILLS = 1_000L;

    public ApiServerRPCClient() {
        client = new NettyRemotingClient(new NettyClientConfig());
        log.info("Success initialized ApiServerRPCClient...");
    }

    public void sendSyncCommand(@NonNull Host host,
                                @NonNull Command rpcCommand) throws RemotingException, InterruptedException {
        sendSyncCommand(host, rpcCommand, DEFAULT_TIME_OUT_MILLS);
    }

    public void sendSyncCommand(@NonNull Host host,
                                @NonNull Command rpcCommand,
                                long timeoutMills) throws RemotingException, InterruptedException {
        client.sendSync(host, rpcCommand, timeoutMills);
    }
}
