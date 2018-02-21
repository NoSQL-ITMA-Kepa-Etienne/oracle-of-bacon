package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {
    private final Jedis jedis;

    public RedisRepository() {
        this.jedis = new Jedis("localhost");
    }

    public List<String> getLastTenSearches() {
        return jedis.lrange("list", 0, 9);
    }

    public void addActor(String actorName) {
        if (jedis.llen("list") >= 10)
            jedis.rpop("list");
        jedis.lpush("list", actorName);
    }
}
