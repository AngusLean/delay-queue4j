package com.doubleysoft.delayquene4j;

import com.doubleysoft.delayquene4j.model.DelayedInfoDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public abstract class DelayMsgServiceImpl implements DelayMsgService {
    private static final String PREFIX_TOPIC = "delay-";
    private final RedisHelper redisHelper;

    @Override
    public void addDelayMessage(DelayedInfoDTO delayedInfoDTO) {
        String topic = PREFIX_TOPIC + delayedInfoDTO.getSystem();
        Long time2Live = System.currentTimeMillis() / 1000 + delayedInfoDTO.getDelayTime();
        redisHelper.zadd(topic, delayedInfoDTO.getMessage(), time2Live);
    }


}
