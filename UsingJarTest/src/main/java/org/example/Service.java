package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Service {
    public static void main(String[] args) {

        if (args.length < 2) {
            //인자가 부족할 경우 종료합니다.
            System.exit(1);
        }

        double x = Integer.parseInt(args[0]);
        double y = Integer.parseInt(args[1]);

        Dataset<Row> resultDf = SelectToQuery(x,y);

        // Show the result
        resultDf.show();
    }

    public static Dataset<Row> SelectToQuery(double x, double y) {

        SparkSession spark = SparkSession.builder()
                .appName("RoadAtPointTab")
                .getOrCreate();

        Dataset<Row> queryResult = spark.sql("SELECT a,b,c FROM TABLE_NAME WHERE ..");


        JavaRDD<Row> rdd = queryResult.toJavaRDD();

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("a", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("b", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("c", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("x", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("y", DataTypes.DoubleType, true));

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> transformedRDD = rdd.map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) {
                // Assuming columns are of type String, Integer, Double for this example
                String a = row.getString(row.fieldIndex("a"));
                int b = row.getInt(row.fieldIndex("b"));
                double c = row.getDouble(row.fieldIndex("c"));

                // Create a new Row with the desired columns
                return RowFactory.create(a, b, c, x, y);
            }
        });

        // Create a new DataFrame from the transformed RDD and the defined schema
        Dataset<Row> resultDataset = spark.createDataFrame(transformedRDD, schema);

        // Stop the Spark session
        spark.stop();

        // Return the resulting DataFrame
        return resultDataset;

    }

}