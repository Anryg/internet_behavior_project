package com.anryg.bigdata;


import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Set;

public class IpSearch {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(IpSearch.class);

    /**
     * 在redis db1数据库中查找IP所在的地址信息
     * @param jedis
     * @param ip
     * @return 给定IP所在范围
     * @throws UnknownHostException
     */
    public static String getAddrByIP(Jedis jedis, String ip)  {
        try {
            jedis.select(1);
            long ipscore = IPUtils.ip2Long(ip);
            Set<Tuple> tuples = jedis.zrangeByScoreWithScores("ipAndAddr", String.valueOf(ipscore),"+inf",0,1);
            String value = "";
            for (Tuple tuple : tuples) {
                value = tuple.getElement();
            }
            String[] valueSplits = value.split("-");
            long begin = IPUtils.ip2Long(valueSplits[0]);
            long end = IPUtils.ip2Long(valueSplits[1]);
            //String[] scope = value.substring(startpos+1,endpos).split(",");
            if(ipscore >= begin && ipscore <= end){
                return value;
            }
            else return "";
        } finally {
            //RedisClientUtils.returnResource(jedis);/**归还到连接池*/

        }
    }
    /**
     * @Author liuxh02
     * @Description  在redis db2数据库中查询ipv4,ipv6地址信息
     * @Date 2020/8/6
     * @Param [jedis, ip]
     * @return java.lang.String
     **/
    public static String getAddr(Jedis jedis, String ip)  {
        jedis.select(2);
        //ip地址转整数
        BigInteger ipscore=null;
        if(ip.contains(":")){
            //ipv6转整数
            ipscore=IPUtils.ipv6ToBigInt(ip);
        }else{
            //ipv4转整数
            ipscore = BigInteger.valueOf(IPUtils.ip2Long(ip));
        }
        Set<Tuple> tuples = jedis.zrangeByScoreWithScores("ipAndAddr",ipscore.toString(),"+inf",0,1);
        String value = "";
        for (Tuple tuple : tuples) {
            value = tuple.getElement();
        }
        String[] valueArray = value.split("-");
        //获取IP和子网掩码
        String ipAndMask=valueArray[0];
        BigInteger start=null;
        BigInteger end=null;
        if(ipAndMask.contains(":")){
            //ipv6地址计算
            BigInteger[] ipv6AndMask = null;
            start=ipv6AndMask[0];
            end=ipv6AndMask[1];
        }else{
            //ipv4地址计算
            long[] ipv4AndMask=IPUtils.getIPLongScope(ipAndMask);
            start= BigInteger.valueOf(ipv4AndMask[0]);
            end= BigInteger.valueOf(ipv4AndMask[1]);
        }
        if(ipscore.compareTo(start)>0 && ipscore.compareTo(end)<0){
            return value;
        }
        else return "";
    }
}
