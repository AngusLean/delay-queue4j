# delay-queue4j
distributed redis-based delay queue written in java. this library is design for
distributed system delay-queue, which does't require **strict exactly delay time**,
but maybe have large data count 。

# design 
view [设计思路](http://anguslean.cn/2019/10/26/DistributeSystem/%E4%B8%80%E7%A7%8D%E5%9F%BA%E4%BA%8ERedis%E7%9A%84%E5%88%86%E5%B8%83%E5%BC%8F%E5%BB%B6%E8%BF%9F%E9%98%9F%E5%88%97%E7%AE%80%E5%8D%95%E5%AE%9E%E7%8E%B0/)

# Usage

currently this project does't upload to maven repository,so you
need to download project and directly use it by source code。

Test Demo:

```$xslt
public class DelayMsgConfigTest1 {
    //最大的延时误差
    private static final int MAX_MARGIN = 3;

    private DelayMsgConfig delayMsgConfig;

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
        RedissonClient redisson = Redisson.create(config);
        delayMsgConfig = new DelayMsgConfig(redisson);
    }

    @Test
    public void addData() throws Exception {
        delayMsgConfig.setCorePoolSize(10);
        delayMsgConfig.setMaximumPoolSize(20);
        delayMsgConfig.begin();
        TimeUnit.SECONDS.sleep(2);
        String system = "DELAY-ATEST1";
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
    }

}
```

