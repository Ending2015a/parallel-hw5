package page_rank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ParsePartitioner extends Partitioner<Text, Text>{
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks){
        return (int)key.toString().charAt(0) % numReduceTasks;
    }

}
