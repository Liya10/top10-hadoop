
package ru.mai.dep806.bigdata.mr;

import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

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
import java.util.ArrayList;
import java.util.List;


public class CoefByTag{

    private static class GroupMapper extends Mapper<Object, Text, Text , Text> {
        private Text outKey = new  Text ();
        private Text outValue = new  Text ();
         @Override 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            String tag=itr.nextToken();
            int year=Integer.parseInt(itr.nextToken());
            double res=Double.parseDouble(itr.nextToken());
            
    
            double b = (double)(year-2008);
            double a = b*b;
            double c = b*res;
            double d = 1.;
            double e = res;                          
            String abcde=Double.toString(a)+" "+Double.toString(b)+" "+Double.toString(c)+" "+Double.toString(d)+" "+Double.toString(e);    

             outKey.set(tag);
            outValue.set(abcde);
            context.write(new  Text (tag),new Text(abcde));
	 
        }
    }

    private static class SumReducer extends Reducer<Text, Text, Text ,Text> {

        private Text outValue = new  Text ();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double a_sum=0, b_sum=0,c_sum=0, d_sum=0, e_sum=0;  
   
            for (Text value: values) {
                StringTokenizer itr = new StringTokenizer(value.toString());
                a_sum+=Double.parseDouble(itr.nextToken());
                b_sum+=Double.parseDouble(itr.nextToken());
                c_sum+=Double.parseDouble(itr.nextToken());
                d_sum+=Double.parseDouble(itr.nextToken());
                e_sum+=Double.parseDouble(itr.nextToken());
             }
             if(d_sum>1){

                 //String res=Double.toString(a_sum)+" "+Double.toString(b_sum)+" "+Double.toString(c_sum)+" "+Double.toString(d_sum)+" "+Double.toString(e_sum);    
                                double res = (c_sum*d_sum-b_sum*e_sum)/ (a_sum*d_sum-b_sum*b_sum)*13+ (a_sum*e_sum-b_sum*c_sum)/ (a_sum*d_sum-b_sum*b_sum);
                 outValue.set(Double.toString(res));
                 context.write(key, outValue);
            }


        }

    }


    public static void main(String[] args) throws Exception {
        final long then = System.nanoTime();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CoefByTag");
        job.setJarByClass(CoefByTag.class);
        job.setMapperClass( GroupMapper.class);
        job.setReducerClass( SumReducer.class);


        // Тип ключа на выходе
        job.setOutputKeyClass(Text.class);
        // Тип значения на выходе
        job.setOutputValueClass(Text.class);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);

        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
        System.out.println("MapReduce Time: " + millis); 
        System.exit(success ? 0 : 1);
    }
}