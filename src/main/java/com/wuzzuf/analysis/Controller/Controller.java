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

import java.io.IOException;

@RestController
public class Controller
{
   
   
    private final DAO dao = new DAO();

   
    @GetMapping("/show_popular_data")
    public  String PopularTile() throws IOException {  return dao.PopularTile();}
                
    @GetMapping("/show_titles_pie_chart")
    public  String  getTitleChart() throws IOException { return dao.getTitleChart();}

    @GetMapping("/show_head_data")
    public String getHead() { return dao.getHead(); }

    @GetMapping("/show_most_demanding_companies")
    public String showMostDamndingCompanies() { return dao.getMostDemandingCompanies(); }

    @GetMapping("/show_companies_pie_chart")
    public  String  showCompaniesPieChart() throws IOException { return dao.getCompanyPieChart(); }

    @GetMapping("/show_years_factoreized")
    public String showYearsFactorized() { return dao.getFactorizedYearsOfExp(); }

    @GetMapping("/apply_kmeans")
    public String applyKMeans() { return dao.kMeansAlgorithm(); }
}
