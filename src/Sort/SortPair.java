package page_rank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;


public class SortPair implements WritableComparable<SortPair>{

    private Text title;
    private double rank;

    public SortPair(){
        title = new Text();
        rank = 0;
    }

    public SortPair(Text title, double rank){
        this.title = title;
        this.rank = rank;
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        title.readFields(in);
        rank = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException{
        title.write(out);
        out.writeDouble(rank);
    }

    @Override
    public int compareTo(SortPair o){
        if(rank == o.rank){
            return title.toString().compareTo(o.title.toString());
        }else if(rank > o.rank){
            return -1;
        }else{
            return 1;
        }
    }

    public Text getTitle(){
        return title;
    }

    public double getRank(){
        return rank;
    }

    public void setTitle(Text title){
        this.title = title;
    }

    public void setRank(double rank){
        this.rank = rank;
    }
}
