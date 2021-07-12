package com.wuzzuf.analysis.Utilities;
import org.apache.commons.codec.binary.Base64;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Displayer {
    public String displayData(List<Row> data, String[] header) {
        HTMLTableBuilder htmlTableBuilder = new HTMLTableBuilder(null, false, 10, header.length);
        htmlTableBuilder.addTableHeader(header);
        for (Row row : data) {
            String[] splittedRow = row.toString()
                    .replace("]", "").replace("[", "")
                    .split(",", header.length);

            htmlTableBuilder.addRowValues(splittedRow);
        }
        return htmlTableBuilder.build();
    }

    public String displayImage(String path) throws IOException
    {
        File file = new File(path);
        FileInputStream fis = new FileInputStream(file);
        byte[] bytes =  new byte[(int)file.length()];
        fis.read(bytes);
        String imgEncoded = new String(Base64.encodeBase64(bytes) , "UTF-8");

        return "<div>" +
                    "<center> <img src=\"data:image/png;base64, "+ imgEncoded +"\" alt=\"Red dot\" /> </center>" +
                "</div>";

    }

}
