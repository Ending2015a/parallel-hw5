package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ResultMapper extends Mapper<Text, OutLink, DoubleWritable, Text>{

    public void map(Text key, OutLink value, Context context) throws IOException, InterruptedException{
    
        //context.write(new SortPair(key, value.getRank()), NullWritable.get() );
        context.write(new DoubleWritable(value.getRank()), key);

    }
}
