package com.anryg.bigdata;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by Anryg on 2018/5/9.
 */
public class RedisClientUtils implements RedisParam {
    private static volatile JedisPool jedisPool = null;/**用连接池进行管理,避免多线程情况下连接redis出现的各种问题*/
    private static volatile Jedis jedis = null;

    /**
     * @DESC: 初始化连接池
    * */
    private static void initPool(){
        JedisPoolConfig config = null;
        try {
            config = new JedisPoolConfig();
            config.setMaxTotal(MAX_ACTIVE);
            config.setMaxIdle(MAX_IDLE);
            config.setMaxWaitMillis(MAX_WAIT);
            config.setTestOnBorrow(TEST_ON_BORROW);//使用时进行扫描，确保都可用
            config.setTestWhileIdle(true);//Idle时进行连接扫描
            config.setTestOnReturn(true);//还回线程池时进行扫描
        } catch (Exception e) {
            throw e;
        }
        /*表示idle object evitor两次扫描之间要sleep的毫秒数
                   config.setTimeBetweenEvictionRunsMillis(30000);
        表示idle object evitor每次扫描的最多的对象数
                    config.setNumTestsPerEvictionRun(10);
        表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
                   config.setMinEvictableIdleTimeMillis(60000);*/
        jedisPool = new JedisPool(config, HOSTS.split(",")[0], PORT, TIMEOUT, PASSWD);
    }
    /**
     *@DESC: 多线程环境下确保只初始化一个连接池
     */
    private static void poolInit() {
        if (jedisPool == null){
            synchronized (RedisClientUtils.class){
                if (jedisPool == null) initPool();
            }
        }
    }

    /**
     * @DESC: 获取连接池对象，适用多线程时，利用其获取多个jedis客户端
     * */
    public static  JedisPool getJedisPool(){
        poolInit();
        return jedisPool;
    }
    /**
     * @DESC: 同步获取Jedis实例，适合单线程
     * @return Jedis
     */
    public static Jedis getSingleRedisClient() {
        poolInit();
        if (jedis == null){
            synchronized (RedisClientUtils.class){
                if (jedis == null) {
                    jedis = jedisPool.getResource();
                }
            }
        }
        return jedis;
    }
    /**
     * @DESC: 释放jedis资源,将资源放回连接池
     * @param jedis
     */
    public static void returnResource(final Jedis jedis) {
        if (jedis != null && jedisPool != null) jedis.close();
    }

    /**
     * @DESC: 删除某个库下的所有数据
     * */
    public static void delDataPerDB(Jedis redis, int dbNum){
        redis.select(dbNum);
        Set<String> keySet = redis.keys("*");
        for (String key:keySet){
            try {
                Set<String> fields = redis.hkeys(key);
                redis.hdel(key,fields.toArray(new String[fields.size()]));//Set转Array
            } catch (Exception e) {
                throw e;
            }finally {
                //redis.close();
            }
        }
    }

    /**
     * @DESC: 存储set对象
    * */
    public static boolean save2RedisBySet(Jedis redis, int redisNo, String key, String[] strArray){
        redis.select(redisNo);
        long count = 0;
        try {
            count = redis.sadd(key,strArray);/**存储不重复的*/
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            redis.close();
        }
        if (count > 0) return true;
        else return false;
    }

    /**
     * @DESC: 用来批量存储key:value
     * @param kvList : 为容器，奇数位的key，偶数位的为value,且总数必须是偶数个
    * */
    public static void save2RedisByKVs(Jedis redis, int redisNo, List<String> kvList){
        redis.select(redisNo);
        try {
            redis.mset(kvList.toArray(new String[kvList.size()]));
        } finally {
            redis.close();
        }
    }
    /**
     * @DESC: 获取set对象的结果
    * */
    public static Set<String> getSetResult(Jedis redis, int redisNo, String key){
        redis.select(redisNo);
        Set<String> scanResult = null;
        try {
            scanResult = redis.smembers(key);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            redis.close();
        }
        return scanResult;
    }

    /**
     *@DESC: 删除指定的key集合（调用时所在环境指定的数据库）
     * */
    public  static void deleteKeys(Jedis redis , List<String> keys){
        redis.del(keys.toArray(new String[keys.size()]));
    }

/**
 * @DESC: 删除指定key(hash类型)下的字段集（调用时所在环境指定的数据库）
 * */
    public   static void deleteFieldByKey(Jedis redis, String key, List<String> fields){
        redis.hdel(key,fields.toArray(new String[fields.size()]));
    }

}
