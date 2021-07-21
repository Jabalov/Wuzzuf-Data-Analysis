package com.wuzzuf.analysis.Business;

import com.wuzzuf.analysis.Utilities.Displayer;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeansSummary;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.Param;
=import org.apache.spark.sql.Dataset;
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
import scala.Option;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

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

   
    private final Dataset<Row> dataset = sparkSession.read()
                                            .option("header", true)
                                            .csv("src\\main\\resources\\wuzzufjobs.csv")
                                            .dropDuplicates()
                                            .filter((FilterFunction<Row>) row -> !row.get(5).equals("null Yrs of Exp"));


    public String getHead()
    {
        List<Row> head = dataset.limit(5).collectAsList();
        return displayer.displayData(head, dataset.columns());
    }

     public String PopularTile () throws IOException
       {
            Dataset<Row> groupedBytitles = dataset.groupBy("Title")
                .count()
                .orderBy(col("count").desc())
                .limit(10);
         
             List<Row> titles = groupedBytitles.collectAsList();
             
             return displayer.displayData(titles, groupedBytitles.columns());
       } 
         
    public String getMostDemandingCompanies()
    {
        Dataset<Row> groupedByCompany = dataset.groupBy("Company")
                .count()
                .orderBy(col("count").desc())
                .limit(10);
        List<Row> groupedByCompanyList = groupedByCompany.collectAsList();

        return displayer.displayData(groupedByCompanyList, groupedByCompany.columns());
    }

    public String getCompanyPieChart() throws IOException
    {
        Dataset<Row> groupedByCompany = dataset.groupBy("Company")
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

        List<Float> counting = new ArrayList<>();

        for(String count : counts)
            counting.add(Float.valueOf(count));
       CategoryChart chart =new CategoryChartBuilder ().width (1800).height (1800).title ("titles bar-Chart").xAxisTitle ("Title").yAxisTitle("count").build ();

        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Vertical);
        
        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        chart.getStyler ().setHasAnnotations (true);
        chart.getStyler ().setStacked (true);
        chart.addSeries("titles", titles, counting);


        BitmapEncoder.saveBitmap(chart, "src\\main\\resources\\title_pie_chart.png", BitmapEncoder.BitmapFormat.PNG);
        return displayer.displayImage("src\\main\\resources\\title_pie_chart.png");
    }

    public String getFactorizedYearsOfExp()
    {
        Dataset<Row> datasetWithFactorizedYears = new StringIndexer()
                .setInputCol("YearsExp")
                .setOutputCol("FactorizedYears")
                .fit(dataset)
                .transform(dataset);

        String[] cols = {"YearsExp", "FactorizedYears"};
        List<Row> years = datasetWithFactorizedYears.select("YearsExp", "FactorizedYears")
                .limit(20)
                .collectAsList();

        return displayer.displayData(years, cols);
    }

    public String kMeansAlgorithm()
    {
        Dataset<Row> data = dataset.as("data");
        String[] cols = {"Title", "Company"};
        String[] factorizedCols = {"TitleFactorized", "CompanyFactorized"};

        for(int i = 0; i < cols.length; i++)
        {
            StringIndexer indexer = new StringIndexer();
            indexer.setInputCol(cols[i]).setOutputCol(factorizedCols[i]);
            data = indexer.fit(data).transform(data);
        }

        for(int i = 0; i < cols.length; i++)
            data = data.withColumn(factorizedCols[i], data.col(factorizedCols[i]).cast("double"));


        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(factorizedCols).setOutputCol("features");
        Dataset<Row> trainData = vectorAssembler.transform(data);

        KMeans kmeans = new KMeans().setK(3).setSeed(1L);
        kmeans.setFeaturesCol("features");
        KMeansModel model = kmeans.fit(trainData);
        
        return "<center>" +
                    "Model Distance Measure: " + model.getDistanceMeasure()
                    + "<br>" +
                    "Number of Features: " + model.numFeatures()
                    + "<br>" +
                    "Number of iterations: " + model.getMaxIter()
                    + "<br>" +
                    "Model Centers:" + Arrays.toString(model.clusterCenters())
                + "</center>";
    }
}
