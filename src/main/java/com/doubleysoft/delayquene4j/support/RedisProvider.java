package com.doubleysoft.delayquene4j.support;

import java.util.List;
import java.util.Set;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
public interface RedisProvider {
    /**
     * add data to a ScoredSorted-Set and a Set. this method must atomic
     *
     * @param setName what will be added to set
     * @param zSetVal Set value and zset key
     * @param msg     zSet value
     * @param ttl     zSet score
     */
    void add2ZSetAndSet(String setName, String zSetVal, String msg, Long ttl);

    /**
     * get data from Set, like {@see href="add2ZSetAndSet"}
     *
     * @param setName
     * @return
     */
    Set<String> getFromSet(String setName);

    /**
     * get data from ScoredSorted-Set by score range
     *
     * @param zSetName ScoredSorted name
     * @param start    score begin, inclusive
     * @param end      score end, inclusive
     * @return
     */
    List<String> getFromZSetByScore(String zSetName, Long start, Long end);

    /**
     * remove from the ScoredSorted Set by zSetName which score between start end end, and
     * insert data to new List which name is listName
     *
     * @param start    zSet score start
     * @param end      zSet score end
     * @param listName new list name
     * @param listMsg  new list data
     */
    void removeFromZSetAndAdd2BlockQueue(String zSetName, Long start, Long end, String listName, List<String> listMsg);

    /**
     * pop data from list, this method must block caller thread
     *
     * @param listName
     * @return
     */
    String blockPopFromList(String listName);
}
