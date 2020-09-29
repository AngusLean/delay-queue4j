package com.doubleysoft.delayquene4j;

import com.doubleysoft.delayquene4j.model.DelayedInfoDTO;
import com.doubleysoft.delayquene4j.support.JsonProvider;
import com.doubleysoft.delayquene4j.support.LockProvider;
import com.doubleysoft.delayquene4j.support.RedisProvider;
import com.doubleysoft.delayquene4j.support.jackson.JacksonProvider;
import com.doubleysoft.delayquene4j.support.reddison.RedissonRedisLockProvider;
import com.doubleysoft.delayquene4j.support.reddison.RedissonRedisProvider;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
public class RedissonClientTest {
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
        RedisProvider redisProvider = new RedissonRedisProvider(redisson);
        LockProvider lockProvider = new RedissonRedisLockProvider(redisson);
        JsonProvider jsonProvider = new JacksonProvider();
        delayMsgConfig = new DelayMsgConfig(redisProvider, lockProvider, jsonProvider);
    }

    @Test
    public void testSeparateStepDelay() throws InterruptedException {
        delayMsgConfig.setCorePoolSize(10);
        delayMsgConfig.setMaximumPoolSize(20);
        delayMsgConfig.begin();
        TimeUnit.SECONDS.sleep(2);
        String system = "DELAY-ATEST3";
        CountDownLatch latch = new CountDownLatch(1);
        //1. step1- register delay message callback
        delayMsgConfig.addDelayCallBack(system, (uuid, message) -> {
            System.out.println("收到消息" + uuid + ", " + message);
            latch.countDown();
        });
        //2. step2- add a delay message, which delayed key must match callback function key
        delayMsgConfig.addDelayMessage(DelayedInfoDTO.builder()
                .delayTime(Math.abs(new Random().nextLong()) % 20)
                .system(system)
                .message(system + String.format("%2d", new Random().nextInt(100)))
                .uuid(UUID.randomUUID().toString())
                .build());
        latch.await(100, TimeUnit.SECONDS);
    }

    @Test
    public void testActualDelay() throws Exception {
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

    @After
    public void finalize() {
        redisson.shutdown();
    }

}