
package ru.mai.dep806.bigdata.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
/**
 * Simple xml parsing utilities.
 */
public class IntTextWritable implements  WritableComparable<IntTextWritable> {
    
    private IntWritable year;
    private Text tag;

    public IntTextWritable(){
        year = new IntWritable();
        tag = new Text();

    }

    public IntTextWritable(int y, String t){
        year = new IntWritable(y);
        tag = new Text(t);

    }
    public void set(int y, String t){
        year = new IntWritable(y);
        tag = new Text(t);
    }
    public IntWritable getYear(){
        return year;
    }
    public Text getTag(){
        return tag;
    }
 
    @Override
    public String toString() {
        return year.toString()+" "+tag.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        tag.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
        tag.write(out);
    }

   @Override
    public int compareTo(IntTextWritable o) {
   
        int compareYear=year.get()-o.getYear().get();
        if(compareYear!=0) return compareYear;

        int compareTag= tag.toString().compareTo(o.getTag().toString());
  
        return compareTag;
    }
}