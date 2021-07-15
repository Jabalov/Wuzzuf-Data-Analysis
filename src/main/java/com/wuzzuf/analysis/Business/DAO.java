package com.wuzzuf.analysis.Business;

import com.wuzzuf.analysis.Utilities.Displayer;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.*;
import org.knowm.xchart.style.AxesChartStyler;
import org.knowm.xchart.style.Styler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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


    public String getHead()
    {
        List<Row> head = dataset.limit(5).collectAsList();
        return displayer.displayData(head, dataset.columns());
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

    public String getLocationBarChart() throws IOException
    {
        Dataset<Row> groupedByLocation = dataset.groupBy("Location")
                .count()
                .orderBy(col("count").desc());

        List<String> Locations = groupedByLocation.select("Location").as(Encoders.STRING()).collectAsList();
        List<String> counted = groupedByLocation.select("count").as(Encoders.STRING()).collectAsList();
        List<Float> counting = new ArrayList();

        for(String s : counted) counting.add(Float.valueOf(s));
        System.out.println();
        CategoryChart charts = new CategoryChartBuilder().width (14366).height (700).title ("Locations Bar_chart").xAxisTitle("Locations").yAxisTitle("frequency").build();
        charts.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        charts.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);
        charts.getStyler().setHasAnnotations(true);
        charts.getStyler().setStacked(true);
        charts.addSeries("Locations", Locations, counting);

        //new SwingWrapper(charts).displayChart();




        BitmapEncoder.saveBitmap(charts, "src\\main\\resources\\Locations Bar_chart.png", BitmapEncoder.BitmapFormat.PNG);
        return displayer.displayImage("src\\main\\resources\\Locations Bar_chart.png");
    }

}
