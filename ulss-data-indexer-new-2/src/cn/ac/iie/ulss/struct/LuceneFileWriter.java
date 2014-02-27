/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.struct;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.lucene.index.IndexWriter;

/**
 *
 * @author liucuili
 */
public class LuceneFileWriter {
    //对于哈希分区会有多个writer，每个分区对应一个物理文件，对应一个writer，因此会有多个writer；  并且有多个SFile对应
    private ConcurrentHashMap<Integer, IndexWriter> writerMap;
    private ConcurrentHashMap<Integer, List<SFile>> sfMap;

    public LuceneFileWriter(ConcurrentHashMap<Integer, IndexWriter> writermap, ConcurrentHashMap<Integer, List<SFile>> sfmap) {
        writerMap = writermap;
        sfMap = sfmap;
    }

    public ConcurrentHashMap<Integer, IndexWriter> getWriterMap() {
        return writerMap;
    }

    public void setWriterMap(ConcurrentHashMap<Integer, IndexWriter> writerMap) {
        this.writerMap = writerMap;
    }

    public ConcurrentHashMap<Integer, List<SFile>> getSfMap() {
        return sfMap;
    }

    public void setSfMap(ConcurrentHashMap<Integer, List<SFile>> sfMap) {
        this.sfMap = sfMap;
    }
}
