/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.iie.struct;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.generic.GenericRecord;

/**
 *
 * @author liucuili
 */
public class RecordNode {
    /*
     *    00 01 10 11   低位代表第一种数据源，高位代表第二种数据源
     * 1  只和第一种数据源匹配上
     * 2  和第二种数据源匹配上
     * 3  和第三种数据源匹配上
     */

    public char state;
    public long arriveTimestamp;
    public GenericRecord genRecord;

    public long getArriveTimestamp() {
        return arriveTimestamp;
    }

    public void setArriveTimestamp(long timestamp) {
        this.arriveTimestamp = timestamp;
    }

    public GenericRecord getGenericRecord() {
        return genRecord;
    }

    public void setGenericRecord(GenericRecord rec) {
        this.genRecord = rec;
    }

    public char getState() {
        return state;
    }

    public void setState(int state) {
        this.state = (char) state;
    }

    public static void main(String[] args) {
        System.out.println('s');
        System.out.println((char) 120 + "sds");
    }
}
