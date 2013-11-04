package cn.ac.ncic.impdatabg.util;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Title: FileService </p>
 * <p>Description: 获取文件 </p>
 * <p>Copyright: Copyright (c) 2007</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */
public class FileService {

    public FileService() {
    }

    /**
     * 在本文件夹下查找
     * @param s String 文件名
     * @return File[] 找到的文件
     */
    public static List getFiles(String s) throws Exception {
        return getFiles("./", s);
    }

    /**
     * 获取文件
     * 可以根据正则表达式查找
     * @param dir String 文件夹名称
     * @param s String 查找文件名，可带*.?进行模糊查询
     * @return File[] 找到的文件
     */
    public static List getFiles(String dir, String s) throws Exception {
        //开始的文件夹
        File file = new File(dir);

        s = s.replace('.', '#');
        s = s.replaceAll("#", "\\\\.");
        s = s.replace('*', '#');
        s = s.replaceAll("#", ".*");
        s = s.replace('?', '#');
        s = s.replaceAll("#", ".?");
        s = "^" + s + "$";

//        System.out.println(s);
        Pattern p = Pattern.compile(s);
        ArrayList list = filePattern(file, p);

//        if (list != null) {
//            File[] rtn = new File[list.size()];
//            list.toArray(rtn);
//            return rtn;
//        }
//
//        return null;
        return list;
    }

    /**
     * 递归地取得当前文件夹及其子文件夹下的所有文件
     * @param file File 起始文件夹
     * @param p Pattern 匹配类型
     * @return ArrayList 当前文件夹及其子文件夹下的所有文件
     */
    private static ArrayList filePatternRecursive(File file, Pattern p) throws Exception {
        try {
            if (file == null) {
                return null;
            } else if (file.isFile()) {
                Matcher fMatcher = p.matcher(file.getName());
                if (fMatcher.matches()) {
                    ArrayList list = new ArrayList();
                    list.add(file);
                    return list;
                }
            } else if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null && files.length > 0) {
                    ArrayList list = new ArrayList();
                    for (int i = 0; i < files.length; i++) {
                        ArrayList rlist = filePattern(files[i], p);
                        if (rlist != null) {
                            list.addAll(rlist);
                        }
                    }
                    if (list.size() > 0) {
                        return list;
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }
        return null;
    }

    /**
     * 取得当前文件夹下的所有文件
     * @param file File 起始文件夹
     * @param p Pattern 匹配类型
     * @return ArrayList 当前文件夹下的文件
     */
    private static ArrayList filePattern(File file, Pattern p) throws Exception {
        try {
            if (file == null) {
                return null;
            } else if (file.isFile()) {
                Matcher fMatcher = p.matcher(file.getName());
                if (fMatcher.matches()) {
                    ArrayList list = new ArrayList();
                    list.add(file);
                    return list;
                }
            } else if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null && files.length > 0) {
                    ArrayList list = new ArrayList();
                    for (int i = 0; i < files.length; i++) {
                        Matcher fMatcher = p.matcher(files[i].getName());
                        if (fMatcher.matches()) {
                            list.add(files[i]);
                        }
                    }
                    if (list.size() > 0) {
                        return list;
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }
        return null;
    }
//    /**
//     * 测试
//     * @param args String[]
//     */
//    public static void main(String[] args) {
//        String dir = "F:\\nb\\";
//        String s = "20*.txt";
//        File[] fileArray = FileService.getFiles(dir, s);
//        System.out.println("ok");
//        for (File fileItem : fileArray) {
//            System.out.println(fileItem.getAbsolutePath());
//        }
//    }
}
