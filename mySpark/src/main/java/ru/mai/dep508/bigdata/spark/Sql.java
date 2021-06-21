package ru.mai.dep508.bigdata.spark;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
/**


 */
public class Sql {

    public static void main(String[] args) throws Exception {

        final long then = System.nanoTime();
        SparkConf sparkConf = new SparkConf().setAppName("Spark Post Test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        String postPath = args[0];
        String outPath = args[1];



        // Чтение пользователей из XML файла и преобразование в объекты Row
        JavaPairRDD<String, Tuple2<Integer, Double> > posts = sparkContext
            .textFile(postPath)
            .map(post -> XmlUtils.parseXmlToMap(post))
            .filter(post -> "1".equals(post.get("PostTypeId")))
            .filter(post -> Integer.parseInt(post.get("ViewCount")) >= 50000)
            .flatMapToPair(post -> {
                int ViewCount=Integer.parseInt(post.get("ViewCount"));
                int year=Integer.parseInt(post.get("CreationDate").substring(0,4));
                int Score = Integer.parseInt(post.get("Score"));
                int AnswerCount = Integer.parseInt(post.get("AnswerCount"));
                String Tags=post.get("Tags");
                double result= (double) ViewCount/10000  + (double)Score/100 + (double)AnswerCount/20;

                List<Tuple2<String, Double> > res = new ArrayList<>();

                String[] tags = Tags.substring(4,Tags.length()-4).split("&gt;&lt;");
                for(String tag: tags) {
                    res.add(new Tuple2<>(Integer.toString(year) + tag, result));
                }
                
                return res.iterator();
            })
            .aggregateByKey(
                0D, 
                (sum, post) -> sum + post,
                (sum1, sum2) -> sum1 + sum2
            )
            .mapToPair(row -> {
                return new Tuple2<>(
                    row._1.substring(4), // tag
                    new Tuple2<>(
                        Integer.parseInt(row._1.substring(0, 4)), // year
                        row._2 // double result
                    )
                );
            });
        
        JavaPairRDD<String, Tuple2<Integer, Double> > posts2020 = posts
            .filter(post -> 2020 == post._2._1);
        
        posts
            .join(posts2020)
            .mapToPair(post -> new Tuple2<>(post._1, post._2._1) ) // <Tag, <Year, Result> >
            .aggregateByKey(
                // (A, B, C, D, E)
                Arrays.asList(0D, 0D, 0D, 0D, 0D),
                // (new Tuple5<Double, Double, Double, Double, Double>(0D, 0D, 0D, 0D, 0D)),

                (coeff, row) -> Arrays.asList(
                    coeff.get(0) + (row._1-2008) * (row._1 - 2008),
                    coeff.get(1) + (row._1-2008),
                    coeff.get(2) + (row._1-2008)*row._2,
                    coeff.get(3) + 1,
                    coeff.get(4) + row._2
                ),
                
                (coeff1, coeff2) -> Arrays.asList(
                    coeff1.get(0) + coeff2.get(0),
                    coeff1.get(1) + coeff2.get(1),
                    coeff1.get(2) + coeff2.get(2),
                    coeff1.get(3) + coeff2.get(3),
                    coeff1.get(4) + coeff2.get(4)
                )
            )
            .filter(row -> row._2.get(3) > 1)
            .mapToPair(row -> {
                double A = row._2.get(2) * row._2.get(3) - row._2.get(1) * row._2.get(4);
                double B = row._2.get(0) * row._2.get(4) - row._2.get(1) * row._2.get(2);
                double tmp = row._2.get(0) * row._2.get(3) - row._2.get(1) * row._2.get(1);
                
                A /= tmp;
                B /= tmp;

                return new Tuple2<> (row._1 ,-A*13-B);//для сортировки я умножила на -1
            })
            .orderBy(row->row._2).taken(10)
             .map (row-> row._1+ Double.toString(row._2))

            // Результат ожидается небольшой, поэтому сливаем все партиции в одну
            .coalesce(1)
            // Создаем writer
            // .write()
            // Используя формат JSON
            // .format("org.apache.spark.sql.json")
            // С перезаписью
            // .mode(SaveMode.Overwrite)
            // Сохраняем в файл (терминальное действие - инициирует запуск всей цепочки)
            .saveAsTextFile(outPath);
        
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
        System.out.println("Spark Time: " + millis); // = something around 1000.
    }
}