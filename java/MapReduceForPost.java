
package ru.mai.dep806.bigdata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.hbase.util.Pair;

public class MapReduceForPost  {

    private static class TokenizerMapper extends Mapper<Object, Text, Pair<IntWritable, Text> , DoubleWritable> {

        private Pair<IntWritable, Text> outKey ;
        private DoubleWritable outValue ;
 


         @Override 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());


	 if("1".equals(row.get("PostTypeId"))){
	     int ViewCount=Integer.parseInt(row.get("ViewCount"));
	     if(ViewCount>=100000){

       
	         int year=Integer.parseInt(row.get("CreationDate").substring(0,4));
	  
	         int Score = Integer.parseInt(row.get("Score"));
	         int AnswerCount = Integer.parseInt(row.get("AnswerCount"));
	         String Tags=row.get("Tags");
                 double result= (double) ViewCount/10000+ (double)Score/100+ (double)AnswerCount/20;
                 outValue = new DoubleWritable(result);

                 String[] tags=Tags.substring(4,Tags.length()-4).split("&gt;&lt;");
		 for(String tag: tags){
                     outKey=new Pair(year, tag);
                     context.write(outKey,outValue);
                     
                 }

                 
	     }
	 }
        }
    }

    private static class DoubSumReducer extends Reducer<Pair<IntWritable, Text> , DoubleWritable, Pair<IntWritable, Text> , DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();
    
        public void reduce(Pair<IntWritable, Text> key, Iterable<DoubleWritable> values, Context context)
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
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduceForPost");
        job.setJarByClass(MapReduceForPost.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(DoubSumReducer.class);
        job.setReducerClass(DoubSumReducer.class);


        // Тип ключа на выходе
        job.setOutputKeyClass(Pair.class);
        // Тип значения на выходе
        job.setOutputValueClass(DoubleWritable.class);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}