package com.doubleysoft.delayquene4j;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.*;
import org.redisson.client.protocol.ScoredEntry;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
        RTransaction transaction = redissonClient.createTransaction(TransactionOptions.defaults());
        try {
            RSet<Object> set = redissonClient.getSet(setName);
            set.add(zSetVal);
            RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet(zSetVal);
            scoredSortedSet.add(ttl, msg);
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
        }
    }

    @Override
    public Set<String> getFromSet(String sSetName) {
        RSet<Object> set = redissonClient.getSet(sSetName);
        return set.readAll().stream().map(row -> row.toString()).collect(Collectors.toSet());
    }

    @Override
    public List<String> getFromZsetByScore(String zSetName, Long start, Long end) {
        RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet(zSetName);
        Collection<ScoredEntry<Object>> scoredEntries = scoredSortedSet.entryRange(start, true, end, true);
        return scoredEntries.stream().map(row -> row.getValue().toString()).collect(Collectors.toList());
    }

    @Override
    public void removeFromZSetAndAdd2List(String zSetName, Long start, Long end, String listName, List<String> listMsg) {
        RTransaction transaction = redissonClient.createTransaction(TransactionOptions.defaults());
        try {
            RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet(zSetName);
            scoredSortedSet.readAll();
            RList<Object> list = redissonClient.getList(listName);
            list.addAll(listMsg);
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
        }
    }
}
