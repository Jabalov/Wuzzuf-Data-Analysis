package com.wuzzuf.analysis.Controller;

import com.wuzzuf.analysis.Business.DAO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@RestController
public class Controller
{
   
   
    private final DAO dao = new DAO();

    @RequestMapping("/show_head_data")
    public String getHead() { return dao.getHead(); }

    @GetMapping("/show_companies_pie_chart")
    public  String  show_companies_pie_chart() throws IOException { return dao.getCompanyPieChart(); }
    

    @GetMapping("/show_popular_title")
    public  String PopularTile() throws IOException {  return dao.PopularTile();}
                
    
      @GetMapping("/show_titles_pie_chart")
    public  String  getTitleChart() throws IOException { return dao.getTitleChart();}
}
