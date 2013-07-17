package com.sweetop.styhadoop;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import javax.servlet.http.HttpServlet;

class MyWritable extends GenericWritable {

    MyWritable(Writable writable) {
        set(writable);
    }

    public static Class<? extends Writable>[] CLASSES=null;

    static {
        CLASSES=  (Class<? extends Writable>[])new Class[]{
                Text.class
        };
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;  //To change body of implemented methods use File | Settings | File Templates.
    }
}