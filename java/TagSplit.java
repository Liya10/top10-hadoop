
package ru.mai.dep806.bigdata.mr;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.TimeUnit ;

public class TagSplit  {

    private static class SplitMapper extends Mapper<Object, Text, IntTextWritable , DoubleWritable> {

        private IntTextWritable outKey = new IntTextWritable ();
        private DoubleWritable outValue = new DoubleWritable ();
 


         @Override 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int year=Integer.parseInt(itr.nextToken());
            String Tags=itr.nextToken();
            double res=Double.parseDouble(itr.nextToken());
            outValue.set(res);
 
            String[] tags=Tags.substring(4,Tags.length()-4).split("&gt;&lt;");
            for(String tag: tags){
                outKey.set(year, tag);
                context.write(outKey,outValue);
                     
            }
	 
        }
    }

    private static class DoubSumReducer extends Reducer<IntTextWritable, DoubleWritable, IntTextWritable , DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();
    
        public void reduce(IntTextWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }

    }


    public static void main(String[] args) throws Exception {
        final long then = System.nanoTime();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TagSplit");
        job.setJarByClass(TagSplit.class);
        job.setMapperClass(SplitMapper.class);
        job.setCombinerClass(DoubSumReducer.class);
        job.setReducerClass(DoubSumReducer.class);


        // Тип ключа на выходе
        job.setOutputKeyClass(IntTextWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(DoubleWritable.class);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);


        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
        System.out.println("MapReduce Time: " + millis); // = something around 1000.


        System.exit(success ? 0 : 1);
    }
}