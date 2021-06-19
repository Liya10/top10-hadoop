
package ru.mai.dep806.bigdata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class FilterPost  {

    private static class TokenizerMapper extends Mapper<Object, Text, IntWritable, TextDoubWritable> {

        private IntWritable outKey = new IntWritable();
        private TextDoubWritable outValue = new TextDoubWritable();
        private StringBuilder buffer = new StringBuilder(250);


         @Override 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());


	 if("1".equals(row.get("PostTypeId"))){
	     int ViewCount=Integer.parseInt(row.get("ViewCount"));
	     if(ViewCount>=50000){

       
	         int year=Integer.parseInt(row.get("CreationDate").substring(0,4));
	         outKey.set(year);
	         int Score = Integer.parseInt(row.get("Score"));
	         int AnswerCount = Integer.parseInt(row.get("AnswerCount"));
	         String Tags=row.get("Tags");
                 double result= (double) ViewCount/10000  + (double)Score/100+ (double)AnswerCount/20;

            
                 outValue.set(Tags, result);
                 context.write(outKey,outValue);
	     }
	 }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Filter Post by PostTypeId and ViewCount");
        job.setJarByClass(FilterPost.class);
        job.setMapperClass(TokenizerMapper.class);



        // Тип ключа на выходе
        job.setOutputKeyClass(IntWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(TextDoubleWritable.class);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}