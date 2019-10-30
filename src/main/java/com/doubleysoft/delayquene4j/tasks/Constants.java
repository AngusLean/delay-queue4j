package com.doubleysoft.delayquene4j.tasks;

import lombok.Data;


@Data
public class Constants {
    /**
     * set queue name, this queue value represent different delay message type,
     * and value is Sorted-Set key
     */
    public static final String ALL_TOPIC_SET_NAME = "delayed-topic";

    /**
     * Sorted-Set key, this queue value represent actual delayed messages
     */
    public static final String ZSET_TOPIC_NAME = "delay-";

    /**
     * actual list of message that need to be handle immediately
     */
    public static final String WAITING_HANDLE_LIST_NAME = "wait_handle_delay_queue";


    /**
     * min pull data period, this param will determine precision,
     * for example, current time is 1000, minPeriod is 5. when a message is arrived now,
     * and it's delay is configured 10006, this item will be handle in time 10005.
     */
//    private Long minPeriod;
}
