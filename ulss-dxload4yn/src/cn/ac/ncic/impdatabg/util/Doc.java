/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author alexmu
 */
public abstract class Doc {

    public List<String> getPropertyNameSet() {
        List<String> propertyNameSet = new ArrayList<String>();
        Field[] propertySet = this.getClass().getDeclaredFields();
        for (Field property : propertySet) {
            propertyNameSet.add(property.getName());
        }
        return propertyNameSet;
    }

    public abstract Doc getDocObj();

    public void setPropertyValue(String pPropertyName, String pValue) {
        try {
            Method method = this.getClass().getMethod("set" + pPropertyName, String.class);
            method.invoke(this, pValue);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public Object getPropertyValue(String pPropertyName) {
        String value = null;
        try {
            Method method = this.getClass().getMethod("get" + pPropertyName);
            value = (String) method.invoke(this);
        } catch (Exception ex) {
            ex.printStackTrace();
            value = null;
        }
        return value;
    }
}
