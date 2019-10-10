package problem_set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class ProblemThree {

    public static class SpatialGroupMapper
            extends Mapper<Text, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        int group_size;

        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] valueArray = value.toString().split(",");
            String flag = valueArray[5].strip().replace("\"Flags\": ", "");
            String eval = valueArray[8].strip().replace("\"Elevation\": ", "");
            context.write(new Text(flag), new Text(eval));
        }
    }

    public static class SpatialJoinReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Add Points and Rectangles to seperate ArrayLists
            int max_elevation = Integer.MIN_VALUE;
            int min_elevation = Integer.MAX_VALUE;

            for (Text v : values) {
                int value = Integer.parseInt(v.toString());
                if (value > max_elevation) {
                    max_elevation = value;
                }
                if (value < min_elevation) {
                    min_elevation = value;
                }
            }
            context.write(key, new Text(String.format("%d,%d", max_elevation, min_elevation)));
        }
    }
        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            conf.set("mapred.max.split.size", "200000");
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(ProblemThree.class);
            job.setMapperClass(SpatialGroupMapper.class);
            job.setReducerClass(SpatialJoinReducer.class);
            job.setInputFormatClass(JSONInputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            JSONInputFormat.addInputPath(job, new Path(args[0])); // "/home/kevin/bigDataManagement/project2/json_data"));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));// "/home/kevin/bigDataManagement/project2/job_json_output"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }