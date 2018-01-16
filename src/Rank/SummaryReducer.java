package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SummaryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    
        double sum = 0;
        long total_num = 0;

        for(DoubleWritable d: values){
            sum += d.get();
            total_num++;
        }

        //context.getCounter("SummaryReducer", key.toString()).setValue(total_num);

        context.write(key, new DoubleWritable(sum));
    }
}
