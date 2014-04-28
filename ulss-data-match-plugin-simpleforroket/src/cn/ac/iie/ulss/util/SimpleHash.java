/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *
 * @author liucuili
 */
public class SimpleHash {

    public static long getSimpleMD5(String val) {
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("SHA");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
        md5.reset();
        byte[] keyBytes = null;
        try {
            keyBytes = val.getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
        }
        md5.update(keyBytes);
        byte[] digest = md5.digest();
        String s = new String(digest);
        return (Math.abs(s.hashCode()));
        /*
         int nTime = digest.length / 4 - 1;
         long rv = ((long) (digest[3 + nTime * 4] & 0xFF) << 24)
         | ((long) (digest[2 + nTime * 4] & 0xFF) << 16)
         | ((long) (digest[1 + nTime * 4] & 0xFF) << 8)
         | (digest[0 + nTime * 4] & 0xFF);
         return rv & 0xffffffffL;
         * */
    }

    public static long getSimpleHash(String val) {
        return Math.abs(val.hashCode());
    }

    public static void main(String[] args) {
        long l = SimpleHash.getSimpleHash("1231啊啊啊啊啊啊啊大双方都") % 32;
        System.out.println(l);
    }
}