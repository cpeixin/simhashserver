package datapark.SimHashSample3;

import datapark.test.FNVHash;
import datapark.utils.HashUtils;
import datapark.utils.RedisUtils;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.math.BigInteger;
import java.util.*;

/**
 * Created by dpliyuan on 2016/2/29.
 */
public class SimHashJudge implements DuplicateJudge {

    private static Logger log = Logger.getLogger(SimHashJudge.class);
    private static RedisUtils redisUtils = new RedisUtils();
    private int HASH_LENGTH = 64;

    /**
     * In this method, use int(32 bits) to store hashcode.
     * use 1 as all words weight for simple reason.
     * use Hamming distance as hashcode distance.
     *
     * @author dpliyuan
     */

    public String duplicate(JSONObject requestDataObj) {
        //get words from request
        String words = (String) requestDataObj.get("words");
        //todo:weight calculate
        String weightStr = (String) requestDataObj.get("weight");
        String url = requestDataObj.get("url").toString();
        if (words == null) {
            log.info("no keywords");
            return null;
        }
        List weightList = getWeightList(weightStr);
        //get simhash of words
        String simhash = getHashCode(words, weightList);
        JSONObject responseObj = getResponseObj(simhash, url);
        return responseObj.toString();
    }

    /**
     * @param simhash
     * @return
     */
    private synchronized JSONObject getResponseObj(String simhash, String url) {
        JSONObject responseObj = new JSONObject();
        /*//todo:cut simhash into 4 segment
        if (redisUtils == null) {
            redisUtils = new RedisUtils();
        }
        Jedis jedis = redisUtils.getJedisFromPool();
        Map map = jedis.hgetAll("simhash");
        Iterator it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            String finger = (String) entry.getValue();
            int hammingDistance = getDistance(finger, simhash);
            if (hammingDistance <= 3) {
                responseObj.put("url", entry.getKey());
                responseObj.put("finger", finger);
                responseObj.put("status", "EXIST");
                log.info("find similar  target :    "+entry.getKey() +"   current : "+url +"simhash code is : "+simhash);
                break;
            }
        }
        //todo:if there is no finger which hammingDistance less then 3 in redis,then we put the request finger into redis
        if (responseObj.length() == 0) {
            jedis.hset("simhash", url, simhash);
            responseObj.put("url", url);
            responseObj.put("finger", simhash);
            responseObj.put("status", "INSERT");
            log.info("insert  current : " + url + "simhash code is : " + simhash);
        }
        redisUtils.returnJedisToPool(jedis);*/

        // simhash cut into 4 segment
        String[] hashs=new String[4];
//        String hash_1 = simhash.substring(0, 16);
//        String hash_2 = simhash.substring(16, 32);
//        String hash_3 = simhash.substring(32, 48);
//        String hash_4 = simhash.substring(48, 64);
        hashs[0]=simhash.substring(0, 16);
        hashs[1] = simhash.substring(16, 32);
        hashs[2] = simhash.substring(32, 48);
        hashs[3] = simhash.substring(48, 64);
        if (redisUtils == null) {
            redisUtils = new RedisUtils();
        }
        Jedis jedis = redisUtils.getJedisFromPool();
        //  选择redis的db
        jedis.select(1);
        boolean isFindSimliari = false;
        long ins1 = System.currentTimeMillis();
        for (int i= 0; i<hashs.length ;i++){
            Map<String, String> hashSets = jedis.hgetAll(hashs[i]);
            long start = System.currentTimeMillis();
            for (Map.Entry<String,String> hashValue :hashSets.entrySet()){
                String finger = hashValue.getKey();
                int hammingDistance = getDistance(finger, simhash);
                if (hammingDistance <=3){
                    long end = System.currentTimeMillis();
                    responseObj.put("url", hashValue.getValue());
                    responseObj.put("finger", finger);
                    responseObj.put("status", "EXIST");
                    log.info("find similar url  : "+hashValue.getValue() +"   current : "+url +" simhash code is : "+ simhash +" hammingDistance is : " +hammingDistance +"compareTime need : " + (end -start));
                    isFindSimliari =true;
                    break;
                }
            }
            if (isFindSimliari){
                break;
            }
        }
        if (!isFindSimliari) {
            long ins2 = System.currentTimeMillis();
            log.info("insert  current : " + url + "simhash code is : " + simhash + " all time is : " + (ins2 - ins1));
            responseObj.put("url", url);
            responseObj.put("finger", simhash);
            responseObj.put("status", "INSERT");
        }
        for (int j = 0; j < hashs.length ; j++) {
            jedis.hset(hashs[j],simhash, url);
        }
        redisUtils.returnJedisToPool(jedis);
        return responseObj;
    }

    /**
     * contert "0" and "1" string to int
     *
     * @param str01
     * @return
     */
    private BigInteger string01ToInt(String str01) {
        BigInteger num;
        num = new BigInteger(str01);
        return num;
    }

    /**
     * maybe in the future,we neet to cut simhash into four parts
     *
     * @param simhash
     * @return
     */
    private List cutSimHash(int simhash) {
        List simhashSegmentsList = new ArrayList();
        String simhashStr = Integer.toBinaryString(simhash);
        String patch = "";
        if (simhashStr.length() < 32) {

            for (int i = 0; i < 32 - simhashStr.length(); i++) {
                patch = patch + "0";
            }
        }
        simhashStr = patch + simhashStr;
        int gap = simhashStr.length() / 4;
        for (int i = 0; i < simhashStr.length(); i += gap) {
            simhashSegmentsList.add(simhashStr.substring(i, i + gap));
        }

        return simhashSegmentsList;
    }

//    public int compareString(String str1, String str2) {
//        System.out.println("SimHash compare string of: \"" + str1 + "\" AND \"" + str2 + "\"");
//        int hash1 = getHashCode(str1,null);
//        int hash2 = getHashCode(str2,null);
//
//        int distance = getDistance(hash1, hash2);
//        System.out.println("SimHash string distance of: \"" + str1 + "\" AND \"" + str2 + "\" is:" + distance);
//        return distance;
//    }

    /**
     * Use hamming distance in this method.
     * Can change to other distance like Euclid distance or p-distance, etc.
     *
     * @return
     */
    public static int getDistance(String finger, String simhash) {

        int distance = 0;

//        System.out.println("finger :" + finger + "\nsimhash:" + simhash);

        for (int i = 0; i < finger.length(); i++) {
            if (finger.charAt(i) != simhash.charAt(i)) {
                distance++;
            }
        }
//        System.out.println("distance:" + distance);
        return distance;
    }

    private List getWeightList(String weightStr) {
        List weightList = new ArrayList();
        String[] weightArray = weightStr.split(",");
        for (int i = 0; i < weightArray.length; i++) {
            double weight = Double.valueOf(weightArray[i]);
            weightList.add(weight);
        }
        return weightList;
    }

    /**
     * get simhashValue according to words
     *
     * @param words
     * @return
     */
    private String getHashCode(String words, List weightList) {
        // TODO Auto-generated method stub
        StringBuffer simhash = new StringBuffer();
        try {
            String[] wordsArray = words.split(",");
            double[] hashBits = new double[HASH_LENGTH];
            for (int i = 0; i < wordsArray.length; i++) {
//                BigInteger hash = HashUtils.hashZHOU(wordsArray[i]);
                BigInteger hash = HashUtils.fnv1aHash64(wordsArray[i]);
//                long val = HashUtils.BKDRHash(wordsArray[i]);
//                BigInteger hash = fromLong2BigInt(val);
//                System.out.println("MINE:"+hash);
                for (int j = 0; j < HASH_LENGTH ; j++) {
                    BigInteger bit = BigInteger.ONE.shiftLeft(FNVHash.HASH_BITS - j - 1);

                    //Different keyword may have different weight. add or minus their weight here.
                    //For simple reason, all weight are assigned as 1 in this method.
                    if (hash.and(bit).signum() != 0) {
                        hashBits[j] += Double.valueOf(weightList.get(i).toString());
                    } else {
                        hashBits[j] -= Double.valueOf(weightList.get(i).toString());
                    }
                }
            }
            for (int i = 0; i < HASH_LENGTH; i++) {
                String bit = hashBits[i] > 0 ? "1" : "0";
                simhash.append(bit);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return simhash.toString();
    }
    private BigInteger fromLong2BigInt(long val){
        BigInteger hash = null;

        String valStr = String.valueOf(val);
        if(valStr.split(".").length==2){
            hash = new BigInteger(valStr.split(".")[0]);
        }else{
            hash = new BigInteger(valStr);
        }

        return  hash;

    }

}
