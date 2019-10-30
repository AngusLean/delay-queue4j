package com.doubleysoft.delayquene4j;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
public class PullHandler implements Runnable {

    private final DelayMsgConfig delayMsgConfig;
    private final ExecutorService executorService;
    private final LockProvider lockProvider;
    private final RedisProvider redisProvider;
    private ScheduledExecutorService timedPullService;

    public PullHandler(DelayMsgConfig delayMsgConfig, ExecutorService executorService, LockProvider lockProvider, RedisProvider redisProvider) {
        this.delayMsgConfig = delayMsgConfig;
        this.executorService = executorService;
        this.lockProvider = lockProvider;
        this.redisProvider = redisProvider;

        timedPullService = Executors.newSingleThreadScheduledExecutor();
        timedPullService.scheduleWithFixedDelay(this, delayMsgConfig.getMinPeriod(), delayMsgConfig.getMinPeriod(), TimeUnit.SECONDS);
    }

    public void doPullAllTopics() {
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
        //query begin score should ensure no time-slice between last pull action and this pull action.
        crt = crt - delayMsgConfig.getMinPeriod();
        crt = crt > 0 ? crt : 0;
        Long range = crt + delayMsgConfig.getMinPeriod();
        //delayed message need to be processed now, but we add it to redis queue for performance
        //in distributed system
        try {
            lockProvider.lock(queueName, Long.MAX_VALUE);
            List<String> fromZSetByScore = redisProvider.getFromZsetByScore(queueName, crt, range);
            log.info("[Delay Queue] find Delayed message:{}", fromZSetByScore);
            redisProvider.removeFromZSetAndAdd2List(queueName, crt, range, DelayMsgConfig.WAITING_HANDLE_LIST_NAME, fromZSetByScore);
        } finally {
            lockProvider.release(queueName);
        }
    }

    @Override
    public void run() {
        try {
            doPullAllTopics();
        } catch (Exception ignore) {
        }
    }
}
