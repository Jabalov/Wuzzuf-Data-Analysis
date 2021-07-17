package com.wuzzuf.analysis.Business;

import com.wuzzuf.analysis.Utilities.Displayer;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.style.Styler;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import java.util.Iterator; 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import java.util.Map.Entry;
 import org.apache.spark.sql.types.StructType;


import static org.apache.spark.sql.functions.col;
import smile.data.DataFrame;

public class DAO
{
    private final Displayer displayer = new Displayer();
    private final SparkSession sparkSession = SparkSession.builder()
                                                    .appName("Wuzzuf-Data-Analysis")
                                                    .master("local[*]")
                                                    .getOrCreate();

    private Dataset<Row> dataset = sparkSession.read()
                                            .option("header", true)
                                            .csv("src\\main\\resources\\wuzzufjobs.csv")
                                              .dropDuplicates()
                                            .filter((FilterFunction<Row>) row -> !row.get(5).equals("null Yrs of Exp"));
//
   
      
       
        
                                   
  
   
     
// LOAD DATASETS
      private Dataset<String> TitleDatAset;
    public String getHead()
    {
        List<Row> head = dataset.collectAsList();
        
        return displayer.displayData(head,dataset.columns());
        
    }

      public  StructType Structre(){
         return dataset.schema();
     }
      public Dataset<Row> datasummary()
    {
      Dataset <Row> Summarize=dataset.select("Title","YearsExp").summary();
      return  Summarize;
   
      }
    
       public String PopularTile ()throws IOException{
           
            Dataset<Row> groupedBytitles= dataset.groupBy("Title")
                .count()
                .orderBy(col("count").desc())
                 .limit(10);
             List<Row> titles =groupedBytitles.collectAsList();
             
             return displayer.displayData(titles, groupedBytitles.columns());
             
             
       } 
            
   
       // Dataset<String> TitleDatAset=dataset.select("Title").drop().as(Encoders.STRING());
        // JavaRDD<String> data=TitleDatAset.javaRDD();
        // JavaRDD<String> words = data.flatMap(new FlatMapFunction<String, String>() {
         //@Override public Iterator<String> call(String s) { return Arrays.asList(s.trim().toLowerCase().replaceAll ("\\p{Punct}", "").split(" ")).iterator(); }
         //                   });
         //   Map<String, Long> result = words.countByValue();
           // for (Map.Entry<String, Long> entry: result.entrySet()) {
            //  System.out.println(entry.getKey() + ":" + entry.getValue());
         //}
            //   return words;
           
        public String getCompanyPieChart() throws IOException
    {
        Dataset<Row> groupedByCompany = dataset.groupBy("Title")
                .count()
                .orderBy(col("count").desc())
                .limit(10);

        List<String> companies = groupedByCompany.select("Company").as(Encoders.STRING()).collectAsList();
        List<String> counts = groupedByCompany.select("count").as(Encoders.STRING()).collectAsList();

        PieChart chart = new PieChartBuilder().width(800).height(800).title("Companies Pie-Chart").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);

        for (int i = 0; i < companies.size() ; i++)
            chart.addSeries(companies.get(i), Integer.parseInt(counts.get(i)));

        BitmapEncoder.saveBitmap(chart, "src\\main\\resources\\company_pie_chart.png", BitmapEncoder.BitmapFormat.PNG);
        return displayer.displayImage("src\\main\\resources\\company_pie_chart.png");
    }
   
    public String getTitleChart() throws IOException
    {
        Dataset<Row> groupedByCompany = dataset.groupBy("Title")
                .count()
                .orderBy(col("count").desc())
                 .limit(10);
        List<String> titles = groupedByCompany.select("Title").as(Encoders.STRING()).collectAsList();
        List<String> counts = groupedByCompany.select("count").as(Encoders.STRING()).collectAsList();

        PieChart chart = new PieChartBuilder().width(1800).height(1800).title("titles Pie-Chart").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideE);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Vertical);

        for (int i = 0; i < titles.size() ; i++)
            chart.addSeries(titles.get(i), Integer.parseInt(counts.get(i)));

        BitmapEncoder.saveBitmap(chart, "src\\main\\resources\\title_pie_chart.png", BitmapEncoder.BitmapFormat.PNG);
        return displayer.displayImage("src\\main\\resources\\title_pie_chart.png");
    }
     
       
   
      
       // JavaRDD<String>
      
        
        // COUNTING
        
    
       
       
       
}


