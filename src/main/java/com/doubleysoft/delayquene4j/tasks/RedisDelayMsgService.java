package com.doubleysoft.delayquene4j.tasks;

import com.doubleysoft.delayquene4j.DelayMsgService;
import com.doubleysoft.delayquene4j.DelayedMsgHandler;
import com.doubleysoft.delayquene4j.model.DelayedInfoDTO;
import com.doubleysoft.delayquene4j.support.JsonProvider;
import com.doubleysoft.delayquene4j.support.RedisProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class RedisDelayMsgService implements DelayMsgService, PullMixin {
    private final RedisProvider redisProvider;
    private final JsonProvider jsonProvider;

    @Override
    public void addDelayMessage(DelayedInfoDTO delayedInfoDTO, DelayedMsgHandler msgHandler) {
        addDelayMessage(delayedInfoDTO);
        addDelayCallBack(delayedInfoDTO.getSystem(), msgHandler);
        log.info("[Delay Queue] Add delayed message:{} to redis", delayedInfoDTO);
    }

    @Override
    public void addDelayMessage(DelayedInfoDTO delayedInfoDTO) {
        String topic = getScoredSetName(delayedInfoDTO.getSystem());
        Long crtInSecond = System.currentTimeMillis() / 1000;
        Long time2Live = crtInSecond + delayedInfoDTO.getDelayTime();
        delayedInfoDTO.setTimestamp(crtInSecond);
        redisProvider.add2ZSetAndSet(Constants.ALL_TOPIC_SET_NAME, topic, jsonProvider.toJSONString(delayedInfoDTO), time2Live);
        log.info("[Delay Queue] Add delayed message:{} to redis", delayedInfoDTO);
    }

    @Override
    public void addDelayCallBack(String system, DelayedMsgHandler msgHandler) {
        String topic = getScoredSetName(system);
        HandlerContext.addMsgHandler(topic, new DelayedMsgHandlerWrapper(msgHandler));
    }

    @RequiredArgsConstructor
    @Slf4j
    static class DelayedMsgHandlerWrapper implements DelayedMsgHandler {
        private final DelayedMsgHandler delayedMsgHandler;

        @Override
        public void handle(String uuid, String message) {
            log.info("[Delay Queue] begin handle delay message:{}, {}", uuid, message);
            try {
                delayedMsgHandler.handle(uuid, message);
                log.info("[Delay Queue] complete handle delay message:{}, {}", uuid, message);
            } catch (Exception e) {
                log.warn("[Delay Queue] handle delay message:{} error", uuid, e);
            }
        }
    }

}
