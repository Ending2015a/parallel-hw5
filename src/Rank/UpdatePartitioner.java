package page_rank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class UpdatePartitioner extends Partitioner<Text, PageInfo>{

    @Override
    public int getPartition(Text key, PageInfo value, int numReduceTasks){
        return (int)key.toString().charAt(0) % numReduceTasks;
    }
}
