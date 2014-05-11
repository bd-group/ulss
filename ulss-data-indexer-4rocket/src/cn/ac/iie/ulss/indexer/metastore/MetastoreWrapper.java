/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.metastore;

import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import iie.metastore.MetaStoreClient;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.hive.metastore.api.FOFailReason;
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

    public static boolean onlineFileLocation(SFile sf, StringBuilder s) {
        boolean isOver = false;
        int errorCount = 0;
        while (!isOver) {
            MetaStoreClient msc = GlobalParas.clientPool.getClient();
            try {
                log.info("will online the file " + sf.getFid());
                msc.client.online_filelocation(sf);
                isOver = true;
                return true;
            } catch (MetaException e) {
                log.error(e, e);
                s.append("online operation is error for " + e.getMessage() + ",and the file_id is " + sf.getFid());
                return false;
            } catch (TException ex) {
                log.error(ex, ex);
                isOver = false;
                errorCount++;
                MetaStoreClient tmp = GlobalParas.clientPool.makesureGetOneNewClient();
                if (tmp != null) {
                    msc = tmp;
                } else {
                    log.error("get null client from the metastore,set file online fail ...");
                }

            } catch (Exception e) {
                log.error(e, e);
                isOver = false;
                errorCount++;
                try {
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            } finally {
                GlobalParas.clientPool.realseOneClient(msc);
            }
            if (errorCount >= 3) {
                return false;
            }
        }
        return true;
    }

    public static boolean setFileBad(long file_id) {
        boolean isOver = false;
        int errorCount = 0;
        while (!isOver) {
            MetaStoreClient msc = GlobalParas.clientPool.getClient();
            try {
                log.info("will set the file to bad " + file_id);
                msc.client.set_loadstatus_bad(file_id);
                isOver = true;
                return true;
            } catch (MetaException e) {
                log.error(e, e);
                return false;
            } catch (TException ex) {
                log.error(ex, ex);
                isOver = false;
                errorCount++;
                MetaStoreClient tmp = GlobalParas.clientPool.makesureGetOneNewClient();
                if (tmp != null) {
                    msc = tmp;
                } else {
                    log.error("get null client from the metastore,set file online fail ...");
                }

            } catch (Exception e) {
                log.error(e, e);
                isOver = false;
                errorCount++;
                try {
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            } finally {
                GlobalParas.clientPool.realseOneClient(msc);
            }
            if (errorCount >= 3) {
                return false;
            }
        }
        return true;
    }

    public static SFile getFile(long file_id, StringBuilder s) {
        /*
         * 实际while循环似乎是没必要的，只在处理最后一个catch(Exception)有用
         */
        boolean isOver = false;
        int errorCount = 0;
        String message = "";
        SFile sf = null;
        while (!isOver) {
            MetaStoreClient msc = GlobalParas.clientPool.getClient();
            try {
                log.info("will get file " + file_id);
                sf = msc.client.get_file_by_id(file_id);
                isOver = true;
                return sf;
            } catch (FileOperationException e) {
                isOver = true;
                if (e.getReason() == FOFailReason.NOTEXIST) {
                    log.error("the file not exist: " + file_id + " " + e, e);
                } else {
                    log.error(e, e);
                }
                return null;
            } catch (TException e) {
                log.error(e, e);
                isOver = false;
                errorCount++;
                MetaStoreClient tmp = GlobalParas.clientPool.makesureGetOneNewClient();
                if (tmp != null) {
                    msc = tmp;
                } else {
                    log.error("get null client from the metastore,set file online fail ...");
                }

            } catch (Exception e) {
                log.error(e, e);
                isOver = false;
                errorCount++;
                message = e.getMessage();
                try {
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            } finally {
                GlobalParas.clientPool.realseOneClient(msc);
            }
            if (errorCount >= 3) {
                return null;
            }
        }
        s = s.append(message);
        return sf;
    }

    public static boolean reopenFile(long file_id) {
        int errorCount = 0;
        while (true) {
            MetaStoreClient msc = GlobalParas.clientPool.getClient();
            try {
                log.info("will reopen file " + file_id);
                msc.client.reopen_file(file_id);
                return true;
            } catch (FileOperationException e) {
                log.error(e, e);
                try {
                    SFile sf = MetastoreWrapper.getFile(file_id, new StringBuilder());
                    if (sf != null && sf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                        return true;
                    } else if (sf != null && !(sf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE)) {
                        return false;
                    } else if (sf == null) {
                        errorCount++;
                        try {
                            Thread.sleep(1000);
                        } catch (Exception ex) {
                            log.error(ex, ex);
                        }
                    } else {
                        return false;
                    }
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            } catch (TException e) {
                log.error(e, e);
                errorCount++;
                MetaStoreClient tmp = GlobalParas.clientPool.makesureGetOneNewClient();
                if (tmp != null) {
                    msc = tmp;
                } else {
                    log.error("get null client from the metastore,set file online fail ...");
                }
            } catch (Exception e) {
                log.error(e, e);
                errorCount++;
                try {
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            } finally {
                GlobalParas.clientPool.realseOneClient(msc);
            }
            if (errorCount >= 3) {
                return false;
            }
        }
    }

    public static boolean closeFile(SFile sf, long doc_num, long file_legth) {
        int errorCount = 0;
        while (true) {
            SFile tmp = null;
            MetaStoreClient msc = GlobalParas.clientPool.getClient();
            try {
                log.info("will close file " + sf.getFid());
                GlobalParas.id2Createindex.remove(sf.getFid());
                tmp = sf;
                log.info("close normal sfile " + sf.toString());
                sf.setRecord_nr(doc_num);
                sf.setLength(file_legth);
                log.info("after update,the close normal sfile is : " + sf.toString());
                msc.client.close_file(sf);

                return true;
            } catch (FileOperationException e) {
                log.error("when close file get warn:" + e, e);
                if (e.getReason() == FOFailReason.NOTEXIST) {
                    log.warn("get file:" + sf.getFid() + " fail,because it does not exist，so it has been closed successfully");
                    return true;
                } else {
                    tmp = getFile(tmp.getFid(), new StringBuilder());
                    if (tmp != null && (tmp.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE)) {
                        log.warn("when close file " + sf.getFid() + " get warn,the file status is " + tmp.getStore_status());
                        return true;
                    } else {
                        return false;
                    }
                }
            } catch (TException e) {
                log.error(e, e);
                msc = GlobalParas.clientPool.makesureGetOneNewClient();
            } catch (Exception e) {
                log.error(e, e);
                errorCount++;
                try {
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            } finally {
                GlobalParas.clientPool.realseOneClient(msc);
            }
            if (errorCount >= 2) {
                return false;
            }
        }
    }

    public static boolean makeSureCloseUnclosedFile(long file_id, long record_num, long length) {
        log.info("try to close the unclosed file: " + file_id + " " + record_num + " " + length);
        StringBuilder sb = new StringBuilder();
        SFile sf = null;
        if ((sf = getFile(file_id, sb)) != null) {
            if (sf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                if (!MetastoreWrapper.closeFile(sf, record_num, length)) {
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

    public synchronized static void retryCloseUnclosedFile(String path) {
        try {
            File file = new File(path);
            String[] ss = file.list();
            if (ss != null) {
                try {
                    for (int i = 0; i < ss.length; i++) {
                        long file_id = Long.parseLong(ss[i]);
                        String s = readFileInfo(path, file_id);
                        log.info("will retry close the file for file " + path + "/" + file_id + ",and the file information is " + s);
                        long record_num = Long.parseLong(s.split("-")[0]);
                        long length = Long.parseLong(s.split("-")[1]);
                        if (MetastoreWrapper.makeSureCloseUnclosedFile(file_id, record_num, length)) {
                            File f = new File(path + "/" + file_id); //删除磁盘上的部分相关信息
                            f.delete();
                            log.info("now close the file " + file_id + " " + f.getAbsolutePath() + " and delete the unresolved information done, is it exists now ? " + f.exists());
                        } else {
                        }
                        //每次只最多只关闭一个文件？？
                        break;
                    }
                } catch (Exception e) {
                    log.error(e);
                }
            }
        } catch (Exception e) {
            log.error(e, e);
        }
    }

    public static void writeFileInfo(String path, long fid, long record_num, long length) {
        try {
            File file = new File(path + "/" + fid + "");
            if (file.exists()) {
                log.warn("when write the unclosed file information for " + fid + " error,the information  has existed.will delete it ");
                file.delete();
            }
            FileOutputStream out = new FileOutputStream(file);
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
        log.info("read file info for " + path + "/" + fid);
        try {
            FileReader f = new FileReader(file);
            BufferedReader bur = new BufferedReader(f);
            while ((s = bur.readLine()) != null) {
                break;
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

    public static void main(String[] args) {
        File f = new File("2_5_1"); //删除磁盘上的部分相关信息
        f.delete();
        while (true) {
            log.info("now close the file " + f.getAbsolutePath() + " and delete the unresolved information done, is it exists now ? " + f.exists());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                System.out.println(ex);
            }
        }
    }
}
