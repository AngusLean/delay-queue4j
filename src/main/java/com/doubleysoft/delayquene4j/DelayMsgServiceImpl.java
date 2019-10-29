package com.doubleysoft.delayquene4j;

import com.doubleysoft.delayquene4j.model.DelayedInfoDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public abstract class DelayMsgServiceImpl implements DelayMsgService {

    private final RedisProvider redisProvider;

    @Override
    public void addDelayMessage(DelayedInfoDTO delayedInfoDTO) {
        String topic = DelayMsgConfig.ZSET_TOPIC_NAME + delayedInfoDTO.getSystem();
        Long time2Live = System.currentTimeMillis() / 1000 + delayedInfoDTO.getDelayTime();
        redisProvider.add2ZSetAndSet(DelayMsgConfig.ALL_TOPIC_SET_NAME, topic, delayedInfoDTO.getMessage(), time2Live);
    }


}
