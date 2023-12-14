package org.peng.spark;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.parser.ParserInterface;

public class SparkDemo {
  public static void main(String[] args) throws ParseException, org.apache.spark.sql.catalyst.parser.ParseException {

    CommandLineParser commandLineParser = new GnuParser();
    Options options = new Options();
    options.addOption("test", "testtt", true, "just test");
    CommandLine commandLine =  commandLineParser.parse(options, args);
    String out = commandLine.getOptionValue("test");
    System.out.println(out);
    // SparkSession spark = SparkSession.builder().master("local").appName(out).getOrCreate();
    // spark.sql("select 1").show();

    ParserInterface parserInterface = new CatalystSqlParser();
    parserInterface.parsePlan("select 1");

  }
}