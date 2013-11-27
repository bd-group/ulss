/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.metastore;

import cn.ac.iie.ulss.indexer.HttpDataHandler;
import cn.ac.iie.ulss.indexer.Indexer;
import iie.metastore.MetaStoreClient;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

/**
 *
 * @author liucuili
 */
public class MetastoreWrapper {

    public static Logger log = Logger.getLogger(MetastoreWrapper.class.getName());

    public static boolean makeSureCloseFile(SFile sf, long doc_num, long file_legth) {
        boolean isOver = false;
        int errorCount = 0;
        while (!isOver) {
            SFile tmp = null;
            try {
                HttpDataHandler.id2Createindex.remove(sf.getFid());

                tmp = sf;
                log.info("close normal sfile " + sf.toString());
                sf.setRecord_nr(doc_num);
                sf.setLength(file_legth);
                log.info("after update,the close normal sfile is : " + sf.toString());
                Indexer.cli.client.close_file(sf);

                isOver = true;
            } catch (FileOperationException e) {
                log.error("when close file get warn:" + e, e);
                tmp = makeSureGetFile(tmp.getFid(), new StringBuilder());
                if (tmp != null && tmp.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED) {
                    return true;
                } else {
                    return false;
                }
            } catch (TException e) {
                log.error(e, e);
                errorCount++;
                MetaStoreClient cli = makeSureGetCli();
                if (cli != null) {
                    Indexer.cli = cli;
                } else {
                    log.error("when get the metastore cli get fetal error !");
                    return false;
                }
            } catch (Exception e) {
                log.error(e, e);
            }
            if (errorCount >= 1) {
                isOver = true;
                return false;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                log.error(ex, ex);
            }
        }
        return true;
    }

    public static boolean makeSureCloseUnclosedFile(long file_id, long record_num, long length) {
        log.info("try to close the unclosed file: " + file_id + " " + record_num + " " + length);
        StringBuilder sb = new StringBuilder();
        SFile sf = null;
        if ((sf = makeSureGetFile(file_id, sb)) != null) {
            if (sf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                if (!MetastoreWrapper.makeSureCloseFile(sf, record_num, length)) {
                    return false;
                }
            } else if (sf.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED) {
                log.warn("when close the unclosed file " + file_id + " fail,it is has been CLOSED ？");
                return true;
            } else if (sf.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
                log.error("when close the unclosed file " + file_id + " fail,it is has been  REPLICATED ？");
                return true;
            } else {
                log.error("when close the unclosed file " + file_id + " fail,it is has been  removed ?");
                return true;
            }
        }
        return true;
    }

    public static MetaStoreClient makeSureGetCli() {
        boolean isOver = false;
        int erroCount = 0;
        String message = "";
        MetaStoreClient cli = null;
        while (!isOver) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
            }
            try {
                cli = new MetaStoreClient(Indexer.metaStoreCientUrl, Indexer.metaStoreCientPort);
                isOver = true;
            } catch (Exception ex) {
                isOver = false;
                erroCount++;
                log.error("when get the metastore client error:" + ex, ex);
            }
            if (erroCount >= 4) {
                return null;
            }
        }
        return cli;
    }

    public static void retryCloseUnclosedFile(String path) {
        try {
            File file = new File(path);
            String[] ss = file.list();
            String s = "";
            if (ss != null) {
                for (int i = 0; i < ss.length; i++) {
                    long file_id = Long.parseLong(ss[i]);
                    s = readFileInfo(path, file_id);
                    long record_num = Long.parseLong(s.split("-")[0]);
                    long length = Long.parseLong(s.split("-")[1]);
                    if (MetastoreWrapper.makeSureCloseUnclosedFile(file_id, record_num, length)) {
                        File f = new File(path + "/" + file_id); //删除磁盘上的部分相关信息
                        f.deleteOnExit();
                        log.info("now close the file " + file_id + " " + f.getAbsolutePath() + " and delete the unresolved information done, is it exists now ? " + f.exists());
                    } else {
                    }
                }
            }
        } catch (Exception e) {
            log.error(e, e);
        }
    }

    public static void writeFileInfo(String path, long fid, long record_num, long length) {
        File file = new File(path + "/" + fid + "");
        if (file.exists()) {
            log.warn("when write the unclosed file information for " + fid + " error,the information  has existed.will delete it ");
            file.delete();
        }
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
            out.write((record_num + "-" + length).getBytes());
            out.close();
        } catch (FileNotFoundException ex) {
            log.error(ex, ex);
        } catch (IOException ex) {
            log.error(ex, ex);
        }
    }

    public static String readFileInfo(String path, long fid) {
        String s = "";
        File file = new File(path + "/" + fid + "");
        try {
            FileReader f = new FileReader(file);
            BufferedReader bur = new BufferedReader(f);
            while ((s = bur.readLine()) != null) {
            }
            f.close();
            bur.close();
        } catch (FileNotFoundException ex) {
            log.error(ex, ex);
        } catch (IOException ex) {
            log.error(ex, ex);
        }
        return s;
    }

    public static boolean makeSureOnlineFileLocation(SFile sf, StringBuilder s) {
        boolean isOver = false;
        int errorCount = 0;
        while (!isOver) {
            try {
                Indexer.cli.client.online_filelocation(sf);
                isOver = true;
            } catch (MetaException e) {
                log.error(e, e);
                s.append("online operation is error for " + e.getMessage() + ",and the file_id is " + sf.getFid());
                return false;
            } catch (TException ex) {
                log.error(ex, ex);
                isOver = false;
                errorCount++;
                MetaStoreClient cli = MetastoreWrapper.makeSureGetCli();
                if (cli != null) {
                    Indexer.cli = cli;
                } else {
                    log.error("when get the metastore cli get fetal error !");
                    return false;
                }
            }
            if (errorCount >= 5) {
                return false;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                log.error(ex, ex);
            }
        }
        return true;
    }

    public static boolean makeSureReopenFile(long file_id) {
        boolean isOver = false;
        int errorCount = 0;
        while (!isOver) {
            try {
                Indexer.cli.client.reopen_file(file_id);
                isOver = true;
            } catch (FileOperationException e) {
                log.error(e, e);
                isOver = false;
                errorCount++;
                try {
                    SFile sf = MetastoreWrapper.makeSureGetFile(file_id, new StringBuilder());
                    if (sf != null && sf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                        isOver = true;
                        break;
                    }
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            } catch (TException e) {
                log.error(e, e);
                isOver = false;
                errorCount++;
                MetaStoreClient cli = MetastoreWrapper.makeSureGetCli();
                if (cli != null) {
                    Indexer.cli = cli;
                } else {
                    log.error("when get the metastore cli get fetal error !");
                    return false;
                }
            } catch (Exception e) {
                log.error(e, e);
                isOver = false;
                errorCount++;
            }
            if (errorCount >= 5) {
                return false;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
            }
        }
        return true;
    }

    public static SFile makeSureGetFile(long file_id, StringBuilder s) {
        boolean isOver = false;
        int errorCount = 0;
        String message = "";
        SFile sf = null;
        while (!isOver) {
            try {
                sf = Indexer.cli.client.get_file_by_id(file_id);
                isOver = true;
            } catch (FileOperationException e) {
                log.error(e, e);
                isOver = false;
                return null;
            } catch (TException e) {
                log.error(e, e);
                isOver = false;
                errorCount++;
                MetaStoreClient cli = MetastoreWrapper.makeSureGetCli();
                if (cli != null) {
                    Indexer.cli = cli;
                } else {
                    log.error("when get the metastore cli get fetal error !");
                    return null;
                }
            } catch (Exception e) {
                log.error(e, e);
                isOver = false;
                errorCount++;
                message = e.getMessage();
            }
            if (errorCount >= 5) {
                return null;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
            }
        }
        s = s.append(message);
        return sf;
    }
}