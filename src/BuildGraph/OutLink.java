package page_rank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.lang.StringBuilder;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

public class OutLink implements Writable{

    private int numOutlink;
    private double rank;
    private double pre_rank;
    private ArrayList<Text> outlink;

    public OutLink(){
        numOutlink = 0;
        rank = 0;
        pre_rank = 1;
        outlink = new ArrayList<Text>();
    }

    public OutLink(int numOutlink, double rank, double pre_rank, ArrayList<Text> outlink){
        this.numOutlink = numOutlink;
        this.rank = rank;
        this.pre_rank = pre_rank;
        this.outlink = outlink;
    }

    public OutLink(OutLink out){
        this(out.numOutlink, out.rank, out.pre_rank, out.outlink);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeDouble(rank);
        out.writeDouble(pre_rank);
        out.writeInt(numOutlink);
        for(Text t: outlink){
            t.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        rank = in.readDouble();
        pre_rank = in.readDouble();
        numOutlink = in.readInt();
        outlink = new ArrayList<Text>();

        Text link = new Text();
        for(int i=0;i<numOutlink;++i){
            link.readFields(in);
            outlink.add(new Text(link));
        }
    }

    public String toString(){
        StringBuilder buf = new StringBuilder();
        buf.append(rank);
        buf.append('\t');
        buf.append(pre_rank);
        buf.append('\t');
        buf.append(numOutlink);
        for(Text t: outlink){
            buf.append('\t');
            buf.append(t.toString());
        }
        return buf.toString();
    }

    public void parse(String str){
        StringTokenizer tkn = new StringTokenizer(str, "\t");
        rank = Double.parseDouble(tkn.nextToken());
        pre_rank = Double.parseDouble(tkn.nextToken());
        numOutlink = Integer.parseInt(tkn.nextToken());
        
        outlink = new ArrayList<Text>();
        for(int i=0;i<numOutlink;++i){
           outlink.add( new Text(tkn.nextToken()) );
        }
    }

    public void newRank(double rank){
        this.pre_rank = this.rank;
        this.rank = rank;
    }

    public void add(Text link){
        outlink.add(link);
    }

    public void setNumOutlink(int num){
        numOutlink = num;
    }

    public void setNumOutlink(){
        numOutlink = outlink.size();
    }

    public void setRank(double rank){
        this.rank = rank;
    }

    public void setOutlink(ArrayList<Text> links){
        outlink = links;
    }

    public ArrayList<Text> getOutlink(){
        return outlink;
    }

    public int getNumOutlink(){
        return numOutlink;
    }

    public double getRank(){
        return rank;
    }

    public boolean isDangling(){
        return (0 == numOutlink);
    }

    public double getError(){
        return Math.abs(rank - pre_rank);
    }
}

