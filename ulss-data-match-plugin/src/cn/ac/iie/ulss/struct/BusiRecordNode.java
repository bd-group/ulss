/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.struct;

import org.apache.avro.generic.GenericRecord;

/**
 *
 * @author liucuili
 */
public class BusiRecordNode {

    public char state;            //0 代表新的，1代表已经匹配成功
    public long arriveTime;       //毫秒数，什么时候此位置信息到达
    public long updateTime;       //毫秒数
    public GenericRecord genRecord;

    public GenericRecord getGenericRecord() {
        return genRecord;
    }

    public void setGenericRecord(GenericRecord rec) {
        this.genRecord = rec;
    }

    public static void main(String[] args) {
        System.out.println('s');
        System.out.println((char) 120 + "sds");
    }
}