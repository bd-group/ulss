/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.utiltools;

import cn.ac.iie.ulss.indexer.worker.Indexer;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import cn.ac.iie.ulss.indexer.constant.Constants;
import iie.index.lucene.SFDirectory;
import iie.metastore.MetaStoreClient;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author liucuili
 */
public class LuceneWriterUtil {

    public static Logger log = Logger.getLogger(LuceneWriterUtil.class.getName());

    public static IndexWriter getSimpleLuceneWriter(String path, boolean isNew) {
        IndexWriter FSWriter = null;
        try {
            File f = new File(path);
            if (!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
            }
            Directory dir = FSDirectory.open(f);
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
            IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_42, analyzer);
            conf.setMaxBufferedDocs(Indexer.luceneBufferDocs);
            conf.setRAMBufferSizeMB(Indexer.luceneBufferRam);
            conf.setMaxThreadStates(Indexer.luceneMaxThreadStates);

            conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            ((TieredMergePolicy) conf.getMergePolicy()).setUseCompoundFile(true);//注意如何设置使用复合索引结构，减小打开的文件数量
            if (IndexWriter.isLocked(dir) && isNew) {
                log.warn("this is the second time for the file " + path + " to create,has something wrong ? ");
                FSWriter = null;
            } else if (IndexWriter.isLocked(dir) && !isNew) {
                log.info("the lucene dir " + dir + " is locked, will unlock it," + "the lock id is " + dir.getLockID());
                IndexWriter.unlock(dir);
                FSWriter = new IndexWriter(dir, conf);
            } else if (!IndexWriter.isLocked(dir) && isNew) {
                FSWriter = new IndexWriter(dir, conf);
            } else if (!IndexWriter.isLocked(dir) && !isNew) {
                FSWriter = new IndexWriter(dir, conf);
            }
        } catch (IOException ex) {
            //假如只读
            FSWriter = null;
            log.error(ex, ex);
        }
        return FSWriter;
    }

    public static IndexWriter getSimpleLuceneWriter(String path) {
        IndexWriter FSWriter = null;
        try {
            File f = new File(path);
            if (!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
            }
            Directory dir = FSDirectory.open(f);
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
            IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_42, analyzer);
            conf.setMaxBufferedDocs(Indexer.luceneBufferDocs);
            conf.setRAMBufferSizeMB(Indexer.luceneBufferRam);
            conf.setMaxThreadStates(Indexer.luceneMaxThreadStates);

            conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            ((TieredMergePolicy) conf.getMergePolicy()).setUseCompoundFile(true);//注意如何设置使用复合索引结构，减小打开的文件数量

            if (IndexWriter.isLocked(dir)) {
                log.info("the lucene dir " + dir + " is locked, will unlock it," + "the lock id is " + dir.getLockID());
                IndexWriter.unlock(dir);
                FSWriter = new IndexWriter(dir, conf);
            } else if (!IndexWriter.isLocked(dir)) {
                FSWriter = new IndexWriter(dir, conf);
            }
        } catch (IOException ex) {
            //假如只读或者磁盘其他什么情况
            FSWriter = null;
            log.error(ex, ex);
        }
        return FSWriter;
    }

    public static void createsFile(String path) throws IOException {
        File file = new File(path);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
        file.createNewFile();
    }

    public static void main(String[] args) {
        try {
            IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(".\\123456", true);
            FSWriter.commit();
            //FSWriter.close();
            //while (true) {
            //    Thread.sleep(5000);
            // }
        } catch (Exception ex) {
            System.out.println(ex);
        }

//        File f = new File(".\\asd\\p\\q\\r");
//        if (!f.getParentFile().exists()) {
//            f.getParentFile().mkdirs();
//        }

    }
}
