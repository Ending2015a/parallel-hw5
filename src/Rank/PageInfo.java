package page_rank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.lang.StringBuilder;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

public class PageInfo implements Writable{

    private OutLink outlink;
    private int numOutlink;
    private double rank;
    private boolean outlinkFlag; //false = numOutlink/rank , true = outlink

    public PageInfo(){
        outlinkFlag = false;
        numOutlink = 0;
        rank = 0;
    }

    public PageInfo(int numOutlink, double rank){
        this.numOutlink = numOutlink;
        this.rank = rank;
        outlinkFlag = false;
    }

    public PageInfo(OutLink outlink){
        this.outlink = new OutLink(outlink);
        outlinkFlag = true;
    }

    public PageInfo(PageInfo info){
        if(info.outlinkFlag == false){
            this.numOutlink = info.numOutlink;
            this.rank = info.rank;
            outlinkFlag = false;
        }else{
            this.outlink = info.outlink;
            outlinkFlag = true;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeBoolean(outlinkFlag);
        if(outlinkFlag){
            outlink.write(out);
        }else{
            out.writeInt(numOutlink);
            out.writeDouble(rank);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        outlinkFlag = in.readBoolean();
        if(outlinkFlag){
            outlink = new OutLink();
            outlink.readFields(in);
        }else{
            numOutlink = in.readInt();
            rank = in.readDouble();
        }
    }

    public String toString(){
        StringBuilder buf = new StringBuilder();
        buf.append(outlinkFlag);
        buf.append('\t');
        if(outlinkFlag){
            buf.append(outlink.toString());
        }else{
            buf.append(rank);
            buf.append('\t');
            buf.append(numOutlink);
        }
        return buf.toString();
    }

    public boolean isOutlink(){
        return outlinkFlag;
    }

    public void setOutlink(OutLink outlink){
        this.outlink = new OutLink(outlink);
        outlinkFlag = true;
    }

    public void setRank(double rank){
        this.rank = rank;
    }

    public void setNumOutlink(int num){
        this.numOutlink = num;
    }

    public OutLink getOutlink(){
        return outlink;
    }

    public double getRank(){
        return rank;
    }

    public int getNumOutlink(){
        return numOutlink;
    }

}

