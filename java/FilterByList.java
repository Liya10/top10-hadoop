
package ru.mai.dep806.bigdata.mr;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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


public class FilterByList  {

    private static class ListMapper extends Mapper<Object, Text, Text ,Text> {

        private Text outKey = new  Text ();
        private Text outValue = new  Text ();
 


         @Override 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String year=itr.nextToken();
            String tag=itr.nextToken();
            String res=itr.nextToken();
            
            outKey.set(tag);
            outValue.set(year+" "+res);
            context.write(outKey,outValue);
	 
        }
    }

    private static class FilterReducer extends Reducer<Text, Text, Text , Text> {

         
    
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> texts = new ArrayList<>();
            boolean flag = false;
            for (Text value: values) {
                texts.add(value.toString());
                StringTokenizer itr = new StringTokenizer(value.toString());
                if("2020".equals(itr.nextToken())){
                    flag=true;
                }
            }
            if(flag){
                for (String text: texts) {
                    context.write(key, new Text(text));
                }
            }

        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FilterByList");
        job.setJarByClass(FilterByList.class);
        job.setMapperClass( ListMapper.class);
        job.setReducerClass( FilterReducer .class);


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
        System.exit(success ? 0 : 1);
    }
}