package com.wuzzuf.analysis.Controller;

import com.wuzzuf.analysis.Business.DAO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class Controller
{
    private final DAO dao = new DAO();

    @RequestMapping("/show_head_data")
    public String getHead() { return dao.getHead(); }

    @RequestMapping("/show_most_demanding_companies")
    public String showMostDamndingCompanies() { return dao.getMostDemandingCompanies(); }

    @GetMapping("/show_companies_pie_chart")
    public  String  showCompaniesPieChart() throws IOException { return dao.getCompanyPieChart(); }
}
