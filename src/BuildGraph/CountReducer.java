package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, IntWritable, Text, LongWritable>{

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        long total = 0;
        for(Object o: values){
            total++;
        }
        context.getCounter("CountNode", key.toString()).setValue(total);

    }
}
