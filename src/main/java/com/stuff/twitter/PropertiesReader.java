package com.stuff.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesReader {
  public static Properties loadTwitterProperties() {
    Properties properties = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();

    try(InputStream resourceStream = loader.getResourceAsStream("twitter/twitter_tokens.properties")) {
      properties.load(resourceStream);

      // get the property value and print it out
    } catch (IOException e) {
      System.out.println("something bad happened");
      e.printStackTrace();
    }
    return properties;

  }
}
