package com.wuzzuf.analysis;

import com.wuzzuf.analysis.Business.DAO;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WuzzufDataAnalysisApplication
{
	public static void main(String[] args)
	{
		SpringApplication.run(WuzzufDataAnalysisApplication.class, args);

		DAO dao = new DAO();
                System.out.println(dao);

	}
}
