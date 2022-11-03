/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.task.api;

import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.FileUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.OSUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * shell command executor
 */
public class ShellCommandExecutor extends AbstractCommandExecutor {

    /**
     * For Unix-like, using sh
     */
    private static final String SH = "sh";

    /**
     * For Windows, using cmd.exe
     */
    private static final String CMD = "cmd.exe";

    /**
     * constructor
     *
     * @param logHandler logHandler
     * @param taskRequest taskRequest
     * @param logger logger
     */
    public ShellCommandExecutor(Consumer<LinkedBlockingQueue<String>> logHandler,
                                TaskExecutionContext taskRequest,
                                Logger logger) {
        super(logHandler, taskRequest, logger);
    }

    public ShellCommandExecutor(LinkedBlockingQueue<String> logBuffer) {
        super(logBuffer);
    }

    @Override
    protected String buildCommandFilePath() {
        // command file
        return String.format("%s/%s.%s", taskRequest.getExecutePath(), taskRequest.getTaskAppId(),
                OSUtils.isWindows() ? "bat" : "command");
    }

    /**
     * create command file if not exists
     *
     * @param execCommand exec command
     * @param commandFile command file
     * @throws IOException io exception
     */
    @Override
    protected void createCommandFileIfNotExists(String execCommand, String commandFile) throws IOException {
        // create if non existence
        logger.info("Begin to create command file:{}", commandFile);

        Path commandFilePath = Paths.get(commandFile);
        if (Files.exists(commandFilePath)) {
            logger.warn("The command file: {} is already exist, will not create a again", commandFile);
            return;
        }

        StringBuilder sb = new StringBuilder();
        if (OSUtils.isWindows()) {
            sb.append("@echo off\n");
            sb.append("cd /d %~dp0\n");
            if (StringUtils.isNotBlank(taskRequest.getEnvironmentConfig())) {
                sb.append(taskRequest.getEnvironmentConfig()).append("\n");
            } else {
                if (taskRequest.getEnvFile() != null) {
                    sb.append("call ").append(taskRequest.getEnvFile()).append("\n");
                }
            }
        } else {
            sb.append("#!/bin/sh\n");
            sb.append("BASEDIR=$(cd `dirname $0`; pwd)\n");
            sb.append("cd $BASEDIR\n");
            if (StringUtils.isNotBlank(taskRequest.getEnvironmentConfig())) {
                sb.append(taskRequest.getEnvironmentConfig()).append("\n");
            } else {
                if (taskRequest.getEnvFile() != null) {
                    sb.append("source ").append(taskRequest.getEnvFile()).append("\n");
                }
            }
        }
        sb.append(execCommand);
        String commandContent = sb.toString();

        FileUtils.createFileWith755(commandFilePath);
        Files.write(commandFilePath, commandContent.getBytes(), StandardOpenOption.APPEND);

        logger.info("Success create command file, command: {}", commandContent);
    }

    @Override
    protected String commandInterpreter() {
        return OSUtils.isWindows() ? CMD : SH;
    }

}
