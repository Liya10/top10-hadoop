
package ru.mai.dep806.bigdata.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Simple xml parsing utilities.
 */
public class IntTextWritable implements Writable {
    
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
}