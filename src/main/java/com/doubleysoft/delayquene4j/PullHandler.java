package com.doubleysoft.delayquene4j;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;


@Slf4j
@RequiredArgsConstructor
public class PullHandler {
    private static final String WAITING_HANDLE_LIST_NAME = "wait_handle_delay_queue";

    private final DelayMsgConfig delayMsgConfig;
    private final ExecutorService executorService;
    private final LockProvider lockProvider;
    private final RedisProvider redisProvider;

    public void pull() {
        Set<String> allTopics = redisProvider.getFromSet(DelayMsgConfig.ALL_TOPIC_SET_NAME);
        allTopics.forEach(row -> {
            executorService.submit(() -> {
                try {
                    doPullTimeOutMsg(row);
                } catch (Exception e) {
                    log.warn("[Delay Queue] Fail in pull message from queue :{}", row, e);
                }
            });
        });
    }

    private void doPullTimeOutMsg(String queueName) {
        long crt = System.currentTimeMillis() / 1000;
        Long range = crt + delayMsgConfig.getMinPeriod();
        //delayed message need to be processed now, but we add it to redis queue for performance
        //in distributed system
        try {
            lockProvider.lock(queueName, Long.MAX_VALUE);
            List<String> fromZSetByScore = redisProvider.getFromZsetByScore(queueName, crt, range);
            log.info("[Delay Queue] find Delayed message:{}", fromZSetByScore);
            redisProvider.removeFromZSetAndAdd2List(queueName, crt, range, WAITING_HANDLE_LIST_NAME, fromZSetByScore);
        } finally {
            lockProvider.release(queueName);
        }

    }
}
