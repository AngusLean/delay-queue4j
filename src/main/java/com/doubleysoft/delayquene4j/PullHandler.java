package com.doubleysoft.delayquene4j;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
@RequiredArgsConstructor
public class PullHandler {
    private final RedisHelper redisHelper;
    private final DelayMsgConfig delayMsgConfig;

    public void pull() {
        List<String> allTopics = redisHelper.getAllTopics();
    }

    private void doPullTimeOutMsg(String queueName) {
        long crt = System.currentTimeMillis() / 1000;
        Long range = crt + delayMsgConfig.getMinPeriod();
        //delayed message need to be processed now, but we add it to redis queue for performance
        //in distributed system
        List<String> delayedMsgs = redisHelper.doZRangeByScore(crt, range);
    }
}
