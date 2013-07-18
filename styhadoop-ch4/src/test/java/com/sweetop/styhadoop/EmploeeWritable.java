package com.sweetop.styhadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-17
 * Time: 下午8:50
 * To change this template use File | Settings | File Templates.
 */
public class EmploeeWritable implements WritableComparable<EmploeeWritable>{

    private Text name;
    private Text role;

    /**
     * 必须有默认的构造器皿，这样Mapreduce方法才能创建对象，然后通过readFields方法从序列化的数据流中读出进行赋值
     */
    public EmploeeWritable() {
        set(new Text(),new Text());
    }

    public EmploeeWritable(Text name, Text role) {
        set(name,role);
    }

    public void set(Text name,Text role) {
        this.name = name;
        this.role = role;
    }

    public Text getName() {
        return name;
    }

    public Text getRole() {
        return role;
    }

    /**
     * 通过成员对象本身的write方法，序列化每一个成员对象到输出流中
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        name.write(dataOutput);
        role.write(dataOutput);
    }

    /**
     * 同上调用成员对象本身的readFields方法，从输入流中反序列化每一个成员对象
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name.readFields(dataInput);
        role.readFields(dataInput);
    }

    /**
     * implements WritableComparable必须要实现的方法,用于比较  排序
     * @param emploeeWritable
     * @return
     */
    @Override
    public int compareTo(EmploeeWritable emploeeWritable) {
        int cmp = name.compareTo(emploeeWritable.name);
        if(cmp!=0){
            return cmp;
        }
        return role.compareTo(emploeeWritable.role);
    }

    /**
     * MapReduce需要一个分割者（Partitioner）把map的输出作为输入分成一块块的喂给多个reduce）
     * 默认的是HashPatitioner，他是通过对象的hashcode函数进行分割，所以hashCode的好坏决定
     * 了分割是否均匀，他是一个很关键性的方法。
     * @return
     */
    @Override
    public int hashCode() {
        return name.hashCode()*163+role.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof EmploeeWritable){
            EmploeeWritable emploeeWritable=(EmploeeWritable)o;
            return name.equals(emploeeWritable.name) && role.equals(emploeeWritable.role);
        }
        return false;
    }

    /**
     * 如果你想自定义TextOutputformat作为输出格式时的输出，你需要重写toString方法
     * @return
     */
    @Override
    public String toString() {
        return name+"\t"+role;
    }

    public static class Comparator extends WritableComparator{
        private static final Text.Comparator TEXT_COMPARATOR= new Text.Comparator();

        protected Comparator() {
            super(EmploeeWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                /**
                 * name是Text类型，Text是标准的UTF-8字节流，
                 * 由一个变长整形开头表示Text中文本所需要的长度，接下来就是文本本身的字节数组
                 * decodeVIntSize返回变长整形的长度，readVInt表示文本字节数组的长度，加起来就是第一个成员name的长度
                 */
                int nameL1= WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
                int nameL2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
                //和compareTo方法一样，先比较name
                int cmp = TEXT_COMPARATOR.compare(b1,s1,nameL1,b2,s2,nameL2);
                if(cmp!=0){
                    return cmp;
                }
                //再比较role
                return TEXT_COMPARATOR.compare(b1,s1+nameL1,l1-nameL1,b2,s2+nameL2,l2-nameL2);
            } catch (IOException e) {
                throw new IllegalArgumentException();
            }
        }

        static {
            //注册raw comprator,更象是绑定，这样MapReduce使用EmploeeWritable时就会直接调用Comparator
            WritableComparator.define(EmploeeWritable.class,new Comparator());
        }
    }


    public static class NameComparator extends WritableComparator{
        private static final Text.Comparator TEXT_COMPARATOR= new Text.Comparator();

        protected NameComparator() {
            super(EmploeeWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int nameL1= WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
                int nameL2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
                return TEXT_COMPARATOR.compare(b1,s1,nameL1,b2,s2,nameL2);
            } catch (IOException e) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if(a instanceof EmploeeWritable && b instanceof  EmploeeWritable){
                return ((EmploeeWritable)a).name.compareTo(((EmploeeWritable)b).name);
            }
            return super.compare(a,b);
        }
    }
}
