package problem_set;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class JSONRecordReader extends RecordReader<Text,Text> {

    // It’s a builtin class that split each file line by line
    LineRecordReader lineRecordReader;
    Text key;
    Text value;

    String temp_value = "";
    int index = 0;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader = new LineRecordReader();
        lineRecordReader.initialize(inputSplit, context);
    }

    // It’s the method that will be used to transform the line into key value
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            return false;
        }
        String line = lineRecordReader.getCurrentValue().toString();
        key = new Text(Integer.toString(index));
        if (line.strip().equalsIgnoreCase("{")){
            while (!line.strip().equalsIgnoreCase("},")) {
                if (!lineRecordReader.nextKeyValue()) {
                    return false;
                }
                line = lineRecordReader.getCurrentValue().toString();
                temp_value += line.strip();
            }

        }
        if (temp_value.equalsIgnoreCase("")){
            return false;
        }
        value = new Text(temp_value);
        temp_value = "";
        index += 1;
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
