package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ResultReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    
        for(Text t: values){
            //context.write(new Text(key.getTitle()), new DoubleWritable(key.getRank()));
            context.write(t, key);
        }
    }
}
