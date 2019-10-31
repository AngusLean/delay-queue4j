package com.doubleysoft.delayquene4j.tasks;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
public interface PullTask {
    default String getLockKey(String queueName) {
        return "LOCK-" + queueName;
    }

    default String getWaitHandleSetName(String keyName) {
        return Constants.WAITING_HANDLE_LIST_NAME + keyName;
    }

    default String getScoredSetName(String setName){
        return Constants.ZSET_TOPIC_NAME + setName;
    }
}
