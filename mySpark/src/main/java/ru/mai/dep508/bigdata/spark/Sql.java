package ru.mai.dep508.bigdata.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.DataFrame;
import java.util.Properties;
import java.util.concurrent.TimeUnit ;

public class Sql {

    private static final String[] POST_FIELDS = new String[]{
             "CreationDate", "ViewCount", "Score", "AnswerCount", "Tags","PostTypeId"
    };

    public static void main(String[] args) throws Exception {

        final long then = System.nanoTime();
        SparkConf sparkConf = new SparkConf().setAppName("Spark Post Sql");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        String postPath = args[0];
        String outPath = args[1];



        // Чтение пользователей из XML файла и преобразование в объекты Row
        JavaRDD<Row> postRdd = sparkContext.textFile(postPath).map(post -> XmlUtils.parseXmlToRow(post, POST_FIELDS));

        // Создание датафрейма и временной таблицы POST из RDD.
        sqlContext.createDataFrame(postRdd, XmlUtils.createDataFrameStruct(POST_FIELDS)).registerTempTable("POST");

        // SQL Запрос 
        sqlContext.sql("with filter_post as( select YEAR(CreationDate) as year, " +
                " ViewCount/10000+Score/100+AnswerCount/20  as result , Tags"+
                " from POST where(PostTypeId==1 and ViewCount>50000 "+
                "), tag_split as (select year, result, tag from filter_post"+
                " LATERAL VIEW explode (split(substr(Tags,5,length(Tags)-8),\"&gt;&lt;\") ) my_Table as tag"+
                "), sum_by_year_and_tag as (select year, tag , sum(result)  result from tag_split  group by year, tag)"+
                ", tag2020 as (select tag from sum_by_year_and_tag where year=2020)"+
                ", filter_by_list as ("+
                      "select year , s.tag, result from sum_by_year_and_tag as s"+
                     " inner join tag2020 as t on t.tag=s.tag"+
                 "),  coef_by_tag as  ("+
                     "select tag, sum ((year-2008)*(year-2008)) as A,"+ 
                      "sum (year-2008) as B, sum ((year-2008)*result) as C, count(*) as D, sum(result ) as E"+
                     " from  filter_by_list group by tag"+
                     ") tag_trand  as ( "+
                     " select tag, (C*D-E*B)/(A*D-B*B) as a, (E*A-B*C)/(D*A-B*B) as b"+
                      "from coef_by_tag where D>1  " +
                      ") select tag, a*13+b as res from  tag_trand order by res desc limit 10;")
            
                .coalesce(2)
         
                .write()
       
                .format("org.apache.spark.sql.json")

                .mode(SaveMode.Overwrite)
   
                .save(outPath);

         final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
          System.out.println("Spark Time: " + millis); // = something around 1000.
    }
}