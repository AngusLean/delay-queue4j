package com.doubleysoft.delayquene4j.tasks;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
public interface PullTask {
    default String getLockKey(String queueName) {
        return "LOCK-" + queueName;
    }

    default String getBlockingKey(String keyName) {
        return Constants.WAITING_HANDLE_LIST_NAME + keyName;
    }
}
