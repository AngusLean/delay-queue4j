package com.doubleysoft.delayquene4j;

import lombok.Data;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
@Data
public class DelaiedInfoDTO {
    //delay time, seconds
    private Long delayTime;

    private String uuid;
}
