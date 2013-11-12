/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.struct;

import cn.ac.iie.util.DynamicBloomFilter;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.SFile;

/**
 *
 * @author liucuili
 */
public class BloomFileWriter {

    private HashMap<Integer, DynamicBloomFilter> blmfWMap;
    private HashMap<Integer, List<SFile>> blSFMap;

    public BloomFileWriter(HashMap<Integer, DynamicBloomFilter> blmfwmp, HashMap<Integer, List<SFile>> blsfmap) {
        this.blmfWMap = blmfwmp;
        this.blSFMap = blsfmap;
    }

    public HashMap<Integer, DynamicBloomFilter> getBlmfWMap() {
        return blmfWMap;
    }

    public void setBlmfWMap(HashMap<Integer, DynamicBloomFilter> blmfWMap) {
        this.blmfWMap = blmfWMap;
    }

    public HashMap<Integer, List<SFile>> getBlSFMap() {
        return blSFMap;
    }

    public void setBlSFMap(HashMap<Integer, List<SFile>> blSFMap) {
        this.blSFMap = blSFMap;
    }
}
