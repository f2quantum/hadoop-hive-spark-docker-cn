package org.peng.spark;


import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class SparkSqlParseDemo {
  public static void main(String[] args) throws ParseException {

    SparkSession spark = SparkSession.builder().master("local").appName("test").getOrCreate();

    spark.sessionState().sqlParser().parsePlan("null");

    spark.sparkContext().getConf().getAppId();
    // setJobGroup is great
    spark.sparkContext().setJobGroup("demo", "gogogogo", false);
    spark.sparkContext().clearJobGroup();
    
    
    TaskContext.get().stageId();

    ParserInterface parserInterface = new CatalystSqlParser();
    LogicalPlan logicalPlan =  parserInterface.parsePlan("insert overwrite table tmp.goggo partition(data_dt=20231212) select aaa, bbb from tmp.test distribute by 1");
    System.out.println(logicalPlan); 
  }
}
