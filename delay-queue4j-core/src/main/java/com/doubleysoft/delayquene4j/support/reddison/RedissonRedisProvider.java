package com.doubleysoft.delayquene4j.support.reddison;

import com.doubleysoft.delayquene4j.support.RedisProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.RedissonShutdownException;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
@Slf4j
@RequiredArgsConstructor
public class RedissonRedisProvider implements RedisProvider {
    private final RedissonClient redissonClient;

    @Override
    public void add2ZSetAndSet(String setName, String zSetVal, String msg, Long ttl) {
        try {
            RSet<Object> set = redissonClient.getSet(setName);
            set.add(zSetVal);
            RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet(zSetVal);
            scoredSortedSet.add(ttl, msg);
        } catch (RedissonShutdownException ignore) {
        } catch (Exception e) {
            log.error("[Delay Queue] Fail in add data to set", e);
        }
    }

    @Override
    public Set<String> getFromSet(String setName) {
        try {
            RSet<Object> set = redissonClient.getSet(setName);
            return set.readAll().stream().map(row -> row.toString()).collect(Collectors.toSet());
        } catch (RedissonShutdownException ignore) {
            return null;
        }
    }

    @Override
    public List<String> getFromZSetByScore(String zSetName, Long start, Long end) {
        try {
            RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet(zSetName);
            Collection<ScoredEntry<Object>> scoredEntries = scoredSortedSet.entryRange(start, true, end, true);
            return scoredEntries.stream().map(row -> row.getValue().toString()).collect(Collectors.toList());
        } catch (RedissonShutdownException ignore) {
            return Collections.EMPTY_LIST;
        }
    }

    @Override
    public void removeFromZSetAndAdd2BlockQueue(String zSetName, Long start, Long end, String listName, List<String> listMsg) {
        try {
            RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet(zSetName);
            scoredSortedSet.removeRangeByScore(start, true, end, true);
            RBlockingQueue<Object> blockingQueue = redissonClient.getBlockingQueue(listName);
            blockingQueue.addAll(listMsg);
        } catch (RedissonShutdownException ignore) {
        } catch (Exception e) {
            log.error("[Delay Queue] Fail in remove data to score-sorted-set", e);
        }
    }

    @Override
    public String blockPopFromList(String listName) {
        RBlockingQueue<Object> blockingQueue;
        Object poll = null;
        try {
            blockingQueue = redissonClient.getBlockingQueue(listName);
            poll = blockingQueue.poll(500, TimeUnit.SECONDS);
        } catch (RedissonShutdownException ignore) {
            return null;
        } catch (InterruptedException e) {
            log.error("[Delay Queue] Fail in pop from block queue:{}", listName, e);
        }
        if (poll == null) {
            return null;
        }
        return poll.toString();
    }
}
