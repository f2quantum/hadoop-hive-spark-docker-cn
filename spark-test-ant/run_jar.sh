for f in $PWD/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

CLASSPATH=${CLASSPATH}:$PWD/build/spark-test-ant-0.0.1.jar

(cd build; java -cp $CLASSPATH org.peng.spark.SparkTest $@)