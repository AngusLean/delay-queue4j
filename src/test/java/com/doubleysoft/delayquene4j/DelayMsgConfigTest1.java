package com.doubleysoft.delayquene4j;

import com.doubleysoft.delayquene4j.model.DelayedInfoDTO;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
public class DelayMsgConfigTest1 {
    //最大的延时误差
    private static final int MAX_MARGIN = 3;

    private DelayMsgConfig delayMsgConfig;
    private RedissonClient redisson;
    @Before
    public void setUp() {
        String host = "redis://redis.dev1.ctstest.com:6379";
        String psd = "password";
        Config config = new Config();
        config.useSingleServer()
                .setAddress(host)
                .setPassword(psd)
                .setDatabase(1);
        // Sync and Async API
        redisson = Redisson.create(config);
        delayMsgConfig = new DelayMsgConfig(redisson);
    }

    @Test
    public void addData() throws Exception {
        delayMsgConfig.setCorePoolSize(10);
        delayMsgConfig.setMaximumPoolSize(20);
        delayMsgConfig.begin();
        TimeUnit.SECONDS.sleep(2);
        Long crt = System.currentTimeMillis() / 1000;
        System.out.println("开始发送消息:" + crt);
        Map<String, Long> target = new HashMap<>();
        Map<String, Long> actualResult = new HashMap<>();
        int testLen = 50;
        CountDownLatch latch = new CountDownLatch(testLen);
        for (int i = 0; i < testLen; i++) {
            String system = "TEST-SYSTEM-" + i;
            DelayedInfoDTO data = DelayedInfoDTO.builder().delayTime(Math.abs(new Random().nextLong()) % 20)
                    .system(system)
                    .message(system + String.format("%2d", new Random().nextInt(100)))
                    .uuid(UUID.randomUUID().toString()).build();
            target.put(data.getUuid(), crt + data.getDelayTime());
            delayMsgConfig.addDelayMessage(data, (uuid, message) -> {
                actualResult.put(uuid, System.currentTimeMillis() / 1000 - target.get(uuid));
                latch.countDown();
            });
        }
        latch.await(100, TimeUnit.SECONDS);
        System.out.println(actualResult);
        List<Long> errorCount = actualResult.values().stream().filter(row -> Math.abs(row) >= MAX_MARGIN).collect(Collectors.toList());
        System.out.println("延迟过大的消息：" + errorCount);
        Assert.assertEquals(0, errorCount.size());
        System.out.println("\n\n=========>>>>>>>>>>>>>\n");
    }

    @Test
    public void addData1() throws InterruptedException {
        delayMsgConfig.setCorePoolSize(10);
        delayMsgConfig.setMaximumPoolSize(20);
        delayMsgConfig.begin();
        TimeUnit.SECONDS.sleep(2);
        String system = "DELAY-ATEST1";
        CountDownLatch latch = new CountDownLatch(1);
        delayMsgConfig.addDelayCallBack(system, (uuid, message) -> {
            System.out.println("收到消息" + uuid + ", " + message);
            latch.countDown();
        });
        delayMsgConfig.addDelayMessage(DelayedInfoDTO.builder()
                .delayTime(Math.abs(new Random().nextLong()) % 20)
                .system(system)
                .message(system + String.format("%2d", new Random().nextInt(100)))
                .uuid(UUID.randomUUID().toString())
                .build());
        latch.await(100, TimeUnit.SECONDS);
        System.out.println("======>");
    }

    @After
    public void finalize() {
        redisson.shutdown();
    }

}