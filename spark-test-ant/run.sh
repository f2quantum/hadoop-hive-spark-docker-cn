for f in $PWD/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

(cd build/classes; java -cp $CLASSPATH org.peng.spark.SparkDemo $@)