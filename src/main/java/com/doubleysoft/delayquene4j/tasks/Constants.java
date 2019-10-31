package com.doubleysoft.delayquene4j.tasks;

import lombok.Data;


@Data
public class Constants {
    /**
     * set queue name, this queue value represent different delay message type,
     * and value is Sorted-Set key
     */
    public static final String ALL_TOPIC_SET_NAME = "DELAY_QUEUE_TOPICS-";

    /**
     * Sorted-Set key, this queue value represent actual delayed messages
     */
    public static final String ZSET_TOPIC_NAME = "DELAY_QUEUE_TOPIC_MSG_SET-";

    /**
     * actual list of message that need to be handle immediately
     */
    public static final String WAITING_HANDLE_LIST_NAME = "DELAY_QUEUE_WAIT_HANDLE_LIST-";

    public static final String LOCK = "LOCK-";

}
