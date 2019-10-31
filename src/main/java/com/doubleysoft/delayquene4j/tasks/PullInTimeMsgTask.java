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
public class PullInTimeMsgTask implements Runnable, PullMixin {
    private final RedisProvider redisProvider;
    private final ExecutorService busiExecutorService;
    private final ExecutorService bgExecutorService;
    private final JsonProvider jsonProvider;

    public PullInTimeMsgTask(RedisProvider redisProvider, JsonProvider jsonProvider, ExecutorService busiExecutorService) {
        this(redisProvider, jsonProvider, busiExecutorService, Executors.newCachedThreadPool());
    }

    public PullInTimeMsgTask(RedisProvider redisProvider, JsonProvider jsonProvider, ExecutorService busiExecutorService, ExecutorService pullOutTimeService) {
        this.redisProvider = redisProvider;
        this.busiExecutorService = busiExecutorService;
        this.jsonProvider = jsonProvider;
        this.bgExecutorService = pullOutTimeService;
        new Thread(this).start();
    }
    @Override
    public void run() {
        while (true) {
            try {
                HandlerContext.setHandlerKeyChangeCallBack(systemKey -> {
                    //each pull handler just pull interest keys
                    bgExecutorService.execute(() -> {
                        doFetchMsg(getWaitHandleSetName(systemKey));
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
        busiExecutorService.execute(() -> {
            HandlerContext.getMsgHandler(getScoredSetName(delayedInfoDTO.getSystem()))
                    .handle(delayedInfoDTO.getUuid(), delayedInfoDTO.getMessage());
        });
    }
}
