/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.worker;

import cn.ac.iie.ulss.struct.CDRRecordNode;
import cn.ac.iie.ulss.util.FileFilter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class RecoverPos {

    public static Logger log = Logger.getLogger(RecoverPos.class.getName());
    public static SimpleDateFormat secondFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    public static void dump(ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> topMap, String path, String region) {
        long bg = System.currentTimeMillis();
        try {
            FileOutputStream outStream = new FileOutputStream(path + "/" + region + "_" + secondFormat.format(new Date()) + ".pos");
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outStream);
            objectOutputStream.writeObject(topMap);
            objectOutputStream.close();
            outStream.close();
            log.info("dump the map position successful,use  " + (System.currentTimeMillis() - bg) + " milis seconds");
        } catch (FileNotFoundException e) {
            log.error(e, e);
        } catch (IOException e) {
            log.error(e, e);
        }
    }

    public static ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> imp(String path) {
        File file = new File(path + "/");
        String[] nameList = file.list(new FileFilter(".*\\.pos"));
        String targetPositionfile = "_0.";
        if (nameList == null || nameList.length == 0) {
            log.info("the pos information file does not exits ...");
            return null;
        } else if (nameList != null) { 
            for (String s : nameList) {
                log.info("now get the position store file " + s);
                if (Long.parseLong(s.split("[_]")[1].split("[.]")[0]) >= Long.parseLong(targetPositionfile.split("[_]")[1].split("[.]")[0])) {
                    targetPositionfile = s;
                }
            }
        }
        FileInputStream freader;
        try {
            log.info("now will use the recover file " + path + "/" + targetPositionfile + " to recover the position ... ");
            freader = new FileInputStream(path + "/" + targetPositionfile);
            ObjectInputStream objectInputStream = new ObjectInputStream(freader);
            ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> map = (ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>) objectInputStream.readObject();
            objectInputStream.close();
            freader.close();
            log.info("imp the position from map successful ... ");
            return map;
        } catch (FileNotFoundException e) {
            log.error(e, e);
        } catch (IOException e) {
            log.error(e, e);
        } catch (ClassNotFoundException e) {
            log.error(e, e);
        }
        return null;
    }

    public static void main(String[] args) {


        String p = "heilongjiang_20131215170034.pos";
        System.out.println(p.split("[_]")[0]);
        System.out.println(p.split("[_]")[1]);


        ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> mp = new ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>();
        for (long i = 0; i < 10; i++) {
            ConcurrentHashMap tmp = new ConcurrentHashMap<String, List<CDRRecordNode>>();
            List<CDRRecordNode> ls = new ArrayList<CDRRecordNode>();
            CDRRecordNode st = new CDRRecordNode();
//            st.a = 0;
//            st.b = 1;
//            st.c = "2";
            System.out.println(st);
            ls.add(st);
            tmp.put("123", ls);
            mp.put(i, tmp);
        }
        RecoverPos.dump(mp, ".", "");
        RecoverPos.imp(".");

    }
}
