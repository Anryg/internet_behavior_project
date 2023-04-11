package com.anryg.bigdata;

//import com.googlecode.ipv6.IPv6Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @DESC: 提供对IP地址数据相关的操作
 * @Author Anryg
 * */

public class IPUtils {
    private static Logger logger = LoggerFactory.getLogger(IPUtils.class);


    /**
     * @DESC: 将本地ip.merge.txt文件中的IP地址导入到redis zset中
     * @param filePath : IP地址与地理位置关系文件
     * @param dbNo : redis的数据库名
     * */
    public static void ipCountryImport(String filePath, int dbNo) throws Exception {
        FileInputStream inputStream = new FileInputStream(filePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null; /*读取文件中的每一行*/
        HashMap<String,Double> map = new HashMap(1024,1); //key为数据体，value为ip范围的结束ip地址
        int i = 0;
        while((line=bufferedReader.readLine()) != null){
            String[] args=line.split("\\|");
            String ipStart=args[0];
            String ipEnd=args[1];
            //Long ipStartLong= IPUtils.ip2Long(ipStart);
            Long ipEndLong= IPUtils.ip2Long(ipEnd); //将每条IP地址范围的结束IP地址，转换为long类型的数值
            String country = args[2];  //获取国家信息
            String province = args[4]; //或者省份信息
            String city = args[5];  //获取城市信息
            String operator = args[6]; //获取运营商信息
            StringBuilder rowBuffer  = new StringBuilder(11); //用来存放组装后的IP地址与地理位置信息
            rowBuffer.append(ipStart).append("-").append(ipEnd).append("-").append(country).append("-")
                    .append(province).append("-").append(city).append("-").append(operator);
            map.put(rowBuffer.toString(),ipEndLong.doubleValue());
            ++i;
            if (i == 1024) {/**每1024个为一批*/
                toRedis(RedisClientUtils.getSingleRedisClient(),map, dbNo,"ipAndAddr");
                map.clear();
                i = 0;
            }
        }
        if (map.size() > 0) toRedis(RedisClientUtils.getSingleRedisClient(),map, dbNo,"ipAndAddr");
    }

    /**
     * @DESC: 将IP转为10进制
     * */
    public static long ip2Long(String ipstr) {
        InetAddress ip = null;
        try {
            ip = InetAddress.getByName(ipstr);
        } catch (UnknownHostException e) {
            logger.error("UnknownHost...",e);
        }
        byte[] octets = ip.getAddress();
        long result = 0;
        for (byte octet : octets) {
            result <<= 8;
            result |= octet & 0xff;
        }
        return result;
    }

    /**
     * @DESC: 经10进制转换成为IPV4地址字符串
     * */
    public static String Long2Ip(long ten) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            sb.insert(0, Long.toString(ten & 0xff));
            if (i < 3) {
                sb.insert(0, '.');
            }
            ten = ten >> 8;
        }
        return sb.toString();
    }

    /**
     * 根据IPV4地址和子网掩码计算IPV4地址范围，例如：192.168.1.53/27 --》3232235808,3232235839
     * @param ipAndMask
     * @return IPV4地址范围
     */
    public static long[] getIPLongScope(String ipAndMask) {
        String[] ipArr = ipAndMask.split("/");
        if (ipArr.length != 2) {
            throw new IllegalArgumentException("invalid ipAndMask with: "
                    + ipAndMask);
        }
        int netMask = Integer.valueOf(ipArr[1].trim());
        if (netMask < 0 || netMask > 32) {
            throw new IllegalArgumentException("invalid ipAndMask with: "
                    + ipAndMask);
        }
        long ipInt = ip2Long(ipArr[0]);
        long netIP = ipInt & (0xFFFFFFFF << (32 - netMask));
        long hostScope = (0xFFFFFFFF >>> netMask);
        return new long[] { netIP, netIP + hostScope };
    }

    /**
     * 根据IPV4地址和子网掩码计算IPV4地址范围，例如：ip：192.168.1.53,子网掩码：255.255.255.224--》3232235808,3232235839
     * @param ipaddr，mask IPV4地址，子网掩码 192.168.1.53，255.255.255.224
     * @return IPV4地址范围字符串
     */
    public static String getIPNetworkAddr(String ipaddr, String mask){
        //IP地址和子网掩码与得到网络地址
        Long ipNetworkAddr = ip2Long(ipaddr)&ip2Long(mask);
        Long ipBroadcastAddr = ((ipNetworkAddr^ip2Long(mask))^0xffffffffL);

        //System.out.println(Long.toBinaryString(ipBroadcastAddr));
        return Long2Ip(ipNetworkAddr+1)+"-->"+Long2Ip(ipBroadcastAddr-1);
    }

    /**
     * ipv6字符串转整数
     * @param ipv6
     * @return
     */
    public static BigInteger ipv6ToBigInt(String ipv6)
    {

        int compressIndex = ipv6.indexOf("::");
        if (compressIndex != -1)
        {
            String part1s = ipv6.substring(0, compressIndex);
            String part2s = ipv6.substring(compressIndex + 1);
            BigInteger part1 = ipv6ToBigInt(part1s);
            BigInteger part2 = ipv6ToBigInt(part2s);
            int part1hasDot = 0;
            char[] ch = part1s.toCharArray();
            for (char c : ch)
            {
                if (c == ':')
                {
                    part1hasDot++;
                }
            }
            // ipv6 has most 7 dot
            return part1.shiftLeft(16 * (7 - part1hasDot )).add(part2);
        }
        String[] str = ipv6.split(":");
        BigInteger big = BigInteger.ZERO;
        for (int i = 0; i < str.length; i++)
        {
            //::1
            if (str[i].isEmpty())
            {
                str[i] = "0";
            }
            big = big.add(BigInteger.valueOf(Long.valueOf(str[i], 16))
                    .shiftLeft(16 * (str.length - i - 1)));
        }
        return big;
    }


    /**
     * @Author liuxh02
     * @Description   整数转为ipv6地址字符串
     * @Date 2020/8/5
     * @Param [big]
     * @return java.lang.String
     **/
    public static String bigIntToipv6(BigInteger big)
    {
        String str = "";
        BigInteger ff = BigInteger.valueOf(0xffff);
        for (int i = 0; i < 8 ; i++)
        {
            str = big.and(ff).toString(16) + ":" + str;

            big = big.shiftRight(16);
        }
        //the last :
        str = str.substring(0, str.length() - 1);

        return str.replaceFirst("(^|:)(0+(:|$)){2,8}", "::");
    }


    /**
     * @DESC: 批量方式写入Redis
     * */
    private static  Long toRedis(Jedis jedis, Map<String,Double> map, int dbno, String key) {
        try {
            jedis.select(dbno);
            return jedis.zadd(key,map);
        } finally {
            RedisClientUtils.returnResource(jedis);
        }

    }


    /**
     * @Author liuxh02
     * @Description  根据ipv6地址和子网掩码计算IP范围，返回数组
     * @Date 2020/8/6
     * @Param 【起始IP，结束IP】
     * @return java.math.BigInteger[]
     **/
/*    public  static BigInteger[]   getIPV6LongScope(String ipv6AndMask ){

        IPv6Network network = IPv6Network.fromString(ipv6AndMask);
        BigInteger start=network.getFirst().toBigInteger();//起始IP
        BigInteger end=network.getLast().toBigInteger();//结束IP
        System.out.println(end);
        return new BigInteger[]{start,end};

    }*/
}
