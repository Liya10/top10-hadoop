
package ru.mai.dep806.bigdata.mr;


import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit ;


public class TopTrand{


    private static class TopNMapper extends Mapper<Object, Text, NullWritable, Text> {

        private TreeMap<Double, Text> topNMap = new TreeMap<>();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String tag = itr.nextToken();
            double res=Double.parseDouble(itr.nextToken());
 
	    topNMap.put(res, new Text(value));

                if (topNMap.size() > 10)  topNMap.remove(topNMap.firstKey());
                
            
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text value : topNMap.values()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    private static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        private TreeMap<Double, Text> topNMap = new TreeMap<>();

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {

                   StringTokenizer itr = new StringTokenizer(value.toString());
         
                   String tag = itr.nextToken();
                   double res=Double.parseDouble(itr.nextToken());
               

                    topNMap.put(res, new Text(value));

                    if (topNMap.size() > 10)  topNMap.remove(topNMap.firstKey());
                    
                
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text value : topNMap.descendingMap().values()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {


        final long then = System.nanoTime();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Tag Trand");
        job.setJarByClass(TopTrand.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);

        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
 

        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
        System.out.println("MapReduce Time: " + millis);
        System.exit(success ? 0 : 1);
    }

}