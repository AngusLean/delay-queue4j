package com.doubleysoft.delayquene4j;

import lombok.Data;


@Data
public class DelayMsgConfig {
    /**
     * min pull data period, this param will determine precision,
     * for example, current time is 1000, minPeriod is 5. when a message is arrived now,
     * and it's delay is configured 10006, this item will be handle in time 10005.
     */
    private Long minPeriod;
}
