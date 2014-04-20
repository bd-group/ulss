/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.constant;

import java.util.HashMap;

/**
 *
 * @author liucuili
 */
public class DataTypeMap {

    public static HashMap<String, Integer> datatypeMap = new HashMap<String, Integer>(100, 0.8f);

    public static void initMap() {
        datatypeMap.put("int", 0);
        datatypeMap.put("array<int>", 1);
        datatypeMap.put("long", 2);
        datatypeMap.put("array<long>", 3);
        datatypeMap.put("bigint", 4);
        datatypeMap.put("array<bigint>", 5);
        datatypeMap.put("timestamp", 6);
        datatypeMap.put("array<timestamp>", 7);
        datatypeMap.put("float", 8);
        datatypeMap.put("array<float>", 9);
        datatypeMap.put("double", 10);
        datatypeMap.put("arra<double>", 11);
        datatypeMap.put("string", 12);
        datatypeMap.put("array<string>", 13);
        datatypeMap.put("binary", 14);
        datatypeMap.put("array<binary>", 15);
        datatypeMap.put("blob", 16);
        datatypeMap.put("array<blob>", 17);
    }
}