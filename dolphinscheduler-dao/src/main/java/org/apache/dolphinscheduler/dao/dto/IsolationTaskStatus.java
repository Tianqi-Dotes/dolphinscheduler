package org.apache.dolphinscheduler.dao.dto;

public enum IsolationTaskStatus {

    ONLINE(0),
    OFFLINE(1),
    ;

    private final int code;

    IsolationTaskStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static IsolationTaskStatus of(int code) {
        for (IsolationTaskStatus value : IsolationTaskStatus.values()) {
            if (value.getCode() == code) {
                return value;
            }
        }
        throw new IllegalArgumentException(String.format("Isolation task status code: %s is invalidated: ", code));
    }
}
