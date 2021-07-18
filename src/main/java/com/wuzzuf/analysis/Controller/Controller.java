package com.wuzzuf.analysis.Controller;

import com.wuzzuf.analysis.Business.DAO;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.IOException;

@RestController
public class Controller
{
    private final DAO dao = new DAO();

    @GetMapping("/show_popular_titles")
    public  String PopularTile() throws IOException {  return dao.getPopularTitles();}
                
    @GetMapping("/show_titles_pie_chart")
    public  String  getTitleChart() throws IOException { return dao.getTitlesChart();}

    @GetMapping("/show_head_data")
    public String getHead() { return dao.getHead(); }

    @GetMapping("/show_most_demanding_companies")
    public String showMostDamndingCompanies() { return dao.getMostDemandingCompanies(); }

    @GetMapping("/show_companies_pie_chart")
    public  String  show_companies_pie_chart() throws IOException { return dao.getCompanyPieChart(); }

    @GetMapping("/show_locations_bar_chart")
    public  String  show_locations_Bar_chart() throws IOException { return dao.getLocationBarChart(); }

    @GetMapping("/show_structure")
    public String showStructure() throws IOException { return  dao.structure(); }
  
    @GetMapping("/show_summary")
    public String showSummary() throws IOException { return dao.getSummary(); }

    @GetMapping("/show_years_factoreized")
    public String showYearsFactorized() { return dao.getFactorizedYearsOfExp(); }

    @GetMapping("/apply_kmeans")
    public String applyKMeans() { return dao.kMeansAlgorithm(); }
}
