/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.util;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

/**
 *
 * @author liucuili
 */
public class FileFilter implements FilenameFilter {

    private Pattern p;

    public FileFilter() {
    }

    public FileFilter(String regex) {
        p = Pattern.compile(regex);
    }

    @Override
    public boolean accept(File file, String name) {
        return p.matcher(name).matches();
        //eturn name.endsWith(".xml");
    }

    public static void main(String[] args) {
        File file = new File(".");
        String[] nameList = file.list(new FileFilter(".*\\.xml"));
        //String[] nameList = file.list(new FileFilter("^bui.*"));
        System.out.println(nameList[0]);
        System.out.println(nameList[1]);

    }
}