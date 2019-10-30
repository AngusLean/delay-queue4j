package com.doubleysoft.delayquene4j.tasks;

import com.doubleysoft.delayquene4j.model.DelayedInfoDTO;
import com.doubleysoft.delayquene4j.support.JsonProvider;
import com.doubleysoft.delayquene4j.support.RedisProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
@Slf4j
public class PullInTimeMsgTask implements Runnable, PullTask {
    private final RedisProvider redisProvider;
    private final ExecutorService executorService;
    private final ExecutorService pullBlockService;
    private final JsonProvider jsonProvider;

    public PullInTimeMsgTask(RedisProvider redisProvider, JsonProvider jsonProvider, ExecutorService executorService) {
        this(redisProvider, jsonProvider, executorService, Executors.newCachedThreadPool());
    }

    public PullInTimeMsgTask(RedisProvider redisProvider, JsonProvider jsonProvider, ExecutorService executorService, ExecutorService pullOutTimeService) {
        this.redisProvider = redisProvider;
        this.executorService = executorService;
        this.jsonProvider = jsonProvider;
        this.pullBlockService = pullOutTimeService;
        new Thread(this).start();
    }
    @Override
    public void run() {
        while (true) {
            try {
                HandlerContext.setHandlerKeyChangeCallBack(systemKey -> {
                    //each pull handler just pull interest keys
                    pullBlockService.execute(() -> {
                        doFetchMsg(getBlockingKey(systemKey));
                    });
                });
            } catch (Exception e) {
                log.warn("[Delay Queue]Fail in fetch delayed message to handle", e);
            }
        }
    }

    /**
     * fetch all message that need to be handle immediately
     */
    private void doFetchMsg(String blockingKeyName) {
        //block fetch
        String msg = redisProvider.blockPopFromList(blockingKeyName);
        if (msg == null || msg.length() == 0) {
            return;
        }
        DelayedInfoDTO delayedInfoDTO;
        try {
            delayedInfoDTO = jsonProvider.fromJSONString(msg, DelayedInfoDTO.class);
        } catch (Exception e) {
            log.warn("[Delay Queue]Fail in parse string:{} to DelayedInfoDTO class", msg);
            return;
        }
        executorService.submit(() -> {
            HandlerContext.getMsgHandler(delayedInfoDTO.getSystem()).handle(delayedInfoDTO.getUuid(), delayedInfoDTO.getMessage());
        });
    }
}
