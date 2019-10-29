package com.doubleysoft.delayquene4j;

import java.util.List;


public abstract class RedisHelper {
    private static final String TOPIC_SET_NAME = "delayed-topic";

    /**
     * a redis zadd command
     *
     * @param topic
     * @param msg
     * @param time2Live actual data live-time, seconds. if the time is less than current
     *                  timestamp, then nothing will do
     */
    public void zadd(String topic, String msg, Long time2Live) {
        //first we add the topic name to set
        doSadd(TOPIC_SET_NAME, topic);
        //then add data to sorted-set with score
        doZadd(topic, msg, time2Live);
    }

    public List<String> getAllTopics() {
        return getDataInSet(TOPIC_SET_NAME);
    }

    public List<String> getFromZSet(Long start, Long end) {
        return doZRangeByScore(start, end);
    }

    public abstract void doSadd(String setName, String value);

    /**
     * add data to a sorted-set with score
     *
     * @param setName
     * @param value
     */
    protected abstract void doZadd(String setName, String value, Long score);

    protected abstract List<String> getDataInSet(String setName);

    protected abstract List<String> doZRangeByScore(Long start, Long end);
}
