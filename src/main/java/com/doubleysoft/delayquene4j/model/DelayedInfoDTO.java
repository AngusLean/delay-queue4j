package com.doubleysoft.delayquene4j.model;

import lombok.Data;


@Data
public class DelayedInfoDTO {
    //delay time, seconds
    private Long delayTime;

    //different system will be stored in different queue.
    private String system;

    //uuid
    private String uuid;

    //actual message which delaymessagehandle will receive
    private String message;
}
