package problem_set;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader;
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
import java.lang.reflect.Array;
import java.util.*;

public class ProblemOne {

    public static class SpatialGroupMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        int group_size;
        String w;
        String[] window;
        int x1;
        int y1;
        int x2;
        int y2;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();

            w = config.get("window");
            if (w.equalsIgnoreCase("null")) {
                window = null;
//                x1 = Integer.parseInt(window[0]);
//                y1 = Integer.parseInt(window[1]);
//                x2 = Integer.parseInt(window[2]);
//                y2 = Integer.parseInt(window[3]);
            } else {
                window = w.split(",");
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line;
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            line = value.toString();

            String[] entries = line.split(",");
            int x_cord = Integer.parseInt(entries[0]);
            int y_cord = Integer.parseInt(entries[1]);

            if (window == null) {
                group_size = 100;

                int x_group = x_cord / group_size;
                int y_group = y_cord / group_size;
                String group = String.format("%d,%d", x_group, y_group);

                if (entries.length == 4) {
                    int y2_group = (y_cord + Integer.parseInt(entries[2])) / group_size;
                    int x2_group = (x_cord + Integer.parseInt(entries[3])) / group_size;
                    int x_diff = x2_group - x_group;
                    int y_diff = y2_group - y_group;
                    String group2;

                    for (int x = 0; x <= x_diff; x++) {
                        for (int y = 0; y <= y_diff; y++) {
                            context.write(new Text(String.format("%d,%d", x_group + x, y_group + y)), new Text(line));
                        }
                    }
                } else {
                    context.write(new Text(group), new Text(line));
                }
            } else if (window != null) {
                if (entries.length == 2) {
                    if ((x_cord >= x1 && x_cord <= x2) && (y_cord >= y1 && y_cord <= y2)) {
                        context.write(new Text(w), new Text(line));
                    }
                } else if (entries.length == 4) {
                    int rect_ymin = y_cord;
                    int rect_ymax = y_cord + Integer.parseInt(entries[2]);
                    int rect_xmin = x_cord;
                    int rect_xmax = x_cord + Integer.parseInt(entries[3]);
                    int window_ymin = Math.min(y1, y2);
                    int window_ymax = Math.max(y1, y2);
                    int window_xmin = Math.min(x1, x2);
                    int window_xmax = Math.max(x1, x2);

                    if ((rect_ymin <= window_ymax && rect_ymax >= window_ymin) && (rect_xmin <= window_xmax && rect_xmax >= window_xmin)) {
                        context.write(new Text(w), new Text(line));
                    }
                }

            }
        }
    }
    public static class SpatialJoinReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Add Points and Rectangles to seperate ArrayLists
            ArrayList<int[]> points = new ArrayList<int[]>();
            ArrayList<int[]> rects = new ArrayList<int[]>();
            for (Text value:values) {
                String[] string_data = value.toString().split(",");
                if (string_data.length == 2){
                    int[] int_data = {Integer.parseInt(string_data[0]),
                                    Integer.parseInt(string_data[1])};
                    points.add(int_data);
                } else if (string_data.length == 4) {
                    int[] int_data = {Integer.parseInt(string_data[0]),
                                    Integer.parseInt(string_data[1]),
                                    Integer.parseInt(string_data[2]),
                                    Integer.parseInt(string_data[3])};
                    rects.add(int_data);
                }
            }

            // Determine which points are in each rectangle
            for (int[] r: rects) {
                for (int[] p: points){
                    if (pointIsIn(r, p)){
                        //context.write(new Text(String.format("%d,%d,%d,%d", r[0], r[1], r[2], r[3])), new Text(String.format("%d,%d", p[0], p[1])));
                    }
                }
            }
        }

        private boolean pointIsIn(int[] rectangle, int[] point){
            if ((point[0] >= rectangle[0] && point[0] <= rectangle[0] + rectangle[3]) && (point[1] >= rectangle[1] && point[1] <= rectangle[1] + rectangle[2])){
                return true;
            } else {
                return false;
            }

        }
    }
    public static void main(String[] args) throws Exception {
        String window;
        if (args.length > 2) {
            window = String.format("%s,%s,%s,%s", args[2], args[3], args[4], args[5]);
        }
        else {
            window = "null";
        }
        System.out.println(window);
        Configuration conf = new Configuration();
        conf.set("window", window);
        Job job = Job.getInstance(conf, "word count");
        job.setNumReduceTasks(4);
        job.setJarByClass(ProblemOne.class);
        job.setMapperClass(SpatialGroupMapper.class);
        job.setReducerClass(SpatialJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));//"/home/kevin/bigDataManagement/project2/data"));
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //"/home/kevin/bigDataManagement/project2/job_one_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}