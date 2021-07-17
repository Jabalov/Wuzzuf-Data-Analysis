package com.wuzzuf.analysis.Controller;

import com.wuzzuf.analysis.Business.DAO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
public class Controller
{
    private final DAO dao = new DAO();

    @RequestMapping("/show_head_data")
    public String getHead() { return dao.getHead(); }

    @GetMapping("/show_companies_pie_chart")
    public  String  show_companies_pie_chart() throws IOException { return dao.getCompanyPieChart(); }

    @GetMapping("/show_Locations_Bar_chart")
    public  String  show_Locations_Bar_chart() throws IOException { return dao.getLocationBarChart(); }

    @GetMapping("/show_Structure")
    public String showstructure() throws IOException
    { return  dao.structure();

    }

    @GetMapping("/show_Summary")
    public String showummary() throws IOException { return dao.getsummary(); }
}
