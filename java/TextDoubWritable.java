
package ru.mai.dep806.bigdata.mr;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
/**
 * Simple xml parsing utilities.
 */
public class TextDoubWritable implements Writable {
    

    private Text tag;
    private DoubleWritable res;


    public TextDoubWritable(){
        tag = new Text();
        res = new DoubleWritable();

    }

    public TextDoubWritable( String t, double r){
        tag = new Text(t);
        res = new DoubleWritable(r);

    }
    public void set(String t, double r){
        tag.set(t);
        res.set(r); 
    }
    @Override
    public String toString() {
        return tag.toString()+" "+res.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tag.readFields(in);
	res.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        tag.write(out);
        res.write(out);
    }
}