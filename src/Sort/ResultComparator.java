package page_rank;

import java.lang.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.io.DoubleWritable;

public class ResultComparator extends WritableComparator{

    public ResultComparator(){
        super(DoubleWritable.class, true);
    }

    public int compare(WritableComparable o1, WritableComparable o2){
    
        DoubleWritable key1 = (DoubleWritable) o1;
        DoubleWritable key2 = (DoubleWritable) o2;

        double d1 = key1.get();
        double d2 = key2.get();

        return d1 < d2 ? 1: -1;
    }
}
