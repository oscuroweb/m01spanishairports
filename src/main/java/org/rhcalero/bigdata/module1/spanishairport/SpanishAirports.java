package org.rhcalero.bigdata.module1.spanishairport;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * SpanishAirports:
 * <p>
 * Obtains the number of different kind of airports in Spain using MapReduce
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 8, 2016
 */
public class SpanishAirports {

    /**
     * Map:
     * <p>
     * Map process: Obtains a pair <key, value> where key is the type of the airport and value is 1
     * </p>
     * 
     * @author Hidalgo Calero, R.
     * @since Oct 8, 2016
     */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        /** ISO Code of Spain. */
        private static final String SPAIN_CODE = "\"ES\"";

        /** Constant of number 1. */
        private static final IntWritable ONE = new IntWritable(1);

        /** Values delimitators . */
        private static final String DELIMITATOR = ",";

        /** Position of type of airport. */
        private static final int AIRPORT_TYPE_POS = 2;

        /** Position of country. */
        private static final int COUNTRY_POS = 8;

        private Text airportType;

        /**
         * Method map.
         * 
         * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object,
         *      org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            // Obtains a list of different fields of line
            String[] lineFields = line.split(DELIMITATOR);
            // Get the code of the country
            String countryCode = lineFields[COUNTRY_POS];

            if (SPAIN_CODE.equals(countryCode)) {
                // If the country is Spain, add the type of airport to the result
                airportType = new Text(lineFields[AIRPORT_TYPE_POS]);
                output.collect(airportType, ONE);
            }
        }
    }

    /**
     * 
     * Reduce:
     * <p>
     * Reduce process: Obtains a pair <key, value> where key is the type of the airport and value is the sum of all
     * airports with this type in Spain
     * </p>
     * 
     * @author Hidalgo Calero, R.
     * @since Oct 8, 2016
     */
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * 
         * Method reduce.
         * 
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator,
         *      org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                Reporter reporter) throws IOException {
            int sum = 0;
            // Sum each value of the list
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    /**
     * 
     * Method main.
     * <p>
     * Execute SpanishAirport program.
     * </p>
     * 
     * @param args Input line arguments
     * @throws Exception Generic exception
     */
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(SpanishAirports.class);
        conf.setJobName("spanishairport");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
