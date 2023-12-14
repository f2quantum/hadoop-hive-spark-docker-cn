package org.peng.spark;

import org.apache.spark.sql.SparkSession;


public class SparkTest {

  public static void main(String args[]) {
    SparkSession spark = SparkSession.builder()
                            .appName("test")
                            .master("spark://master:7077")
                            .getOrCreate();
    spark.sql("select 1").show();
    
  }
  
}
