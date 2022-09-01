package com.anryg.bigdata;



/**
 * Created by Anryg on 2018/5/9.
 * @DESC: 提供Redis的基础属性配置
 */
public interface RedisParam {
    String HOSTS = "192.168.211.106";/**redis服务器列表，目前为单点*/
    int PORT = 6379;
    String PASSWD = "pcl@2020";
    //可用连接实例的最大数目，默认值为8；
    //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)
    int MAX_ACTIVE = 1500;
    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8
    int MAX_IDLE = 100;
    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException
    int MAX_WAIT = 100 * 1000;
    int TIMEOUT = 100 * 1000;//超时时间
    //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    boolean TEST_ON_BORROW = true;
}
