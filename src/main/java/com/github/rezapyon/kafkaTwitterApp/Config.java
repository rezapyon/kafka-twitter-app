package com.github.rezapyon.kafkaTwitterApp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

public class Config {
    String consumerKey;
    String consumerSecret;
    String token;
    String secret;
    InputStream inputStream;

    public void initiateProperties() throws IOException {

        try {
            Properties prop = new Properties();
            String propFileName = "config.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            Date time = new Date(System.currentTimeMillis());

            consumerKey = prop.getProperty("consumerKey");
            consumerSecret = prop.getProperty("consumerSecret");
            token = prop.getProperty("token");
            secret = prop.getProperty("secret");

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            inputStream.close();
        }
    }
}
