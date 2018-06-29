package my.fenci;
import java.io.Serializable;


import java.io.DataInput;  
import java.io.DataOutput;  
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import org.apache.lucene.analysis.Analyzer;  
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;  
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.io.Writable;  
public class Fenci {
	public Fenci() {
	}
	
    public static void main(String[] args) throws Exception {
    	 Configuration conf = new Configuration();
        String[] otherArgs =  new String[]{"hdfs://master:9000/qingxi/out/1/part-r-00000"};
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver","jdbc:mysql://127.0.0.1:3306/BigData", "root", "123456");  
 		 // 配置作业
// 首先创建一个Job类，然后装载需要的各个类
        Job job = Job.getInstance(conf, "Fenci and WordCount");
        job.setJarByClass(Fenci.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(IntSumReducer.class);
       
        job.setOutputKeyClass(TblsWritable.class);
        job.setOutputValueClass(TblsWritable.class);
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(DBOutputFormat.class);  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        DBOutputFormat.setOutput(job, "kwcloud", "kw", "num");  
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class TblsWritable implements Writable, DBWritable {  
        String kw;  
        int num;  
        public TblsWritable() {  
        }  
        public TblsWritable(String kw, int num) {  
            this.kw = kw;  
            this.num = num;  
        }  
        @Override  
        public void write(PreparedStatement statement) throws SQLException {  
            statement.setString(1, this.kw);  
            statement.setInt(2, this.num);  
        }  
        @Override  
        public void readFields(ResultSet resultSet) throws SQLException {  
            this.kw = resultSet.getString(1);  
            this.num = resultSet.getInt(2);  
        }  
        @Override  
        public void write(DataOutput out) throws IOException {  
            out.writeUTF(this.kw);  
            out.writeInt(this.num);  
        }  
        @Override  
        public void readFields(DataInput in) throws IOException {  
            this.kw = in.readUTF();  
            this.num = in.readInt();  
        }  
        public String toString() {  
            return new String(this.kw + " " + this.num);  
        }  
    } 
    public static class IntSumReducer extends Reducer<Text, IntWritable, TblsWritable, TblsWritable> {
        private IntWritable result = new IntWritable();
 
        public IntSumReducer() {
        }

        @Override 
        public void reduce(Text key, Iterable<IntWritable> values, Context context)  
                throws IOException, InterruptedException {  
        	int sum = 0;
 
            IntWritable val;
            for(Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
                val = (IntWritable)i$.next();
            }
 
            this.result.set(sum);
            TblsWritable tw = new TblsWritable(key.toString(), sum);
            System.out.print(tw.toString() + "***************");
            context.write(tw, null);
        }
    }
 
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
 
        public TokenizerMapper() {
        }
        
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        	String line = value.toString();
            String[] datas=line.split("\t");
            String toFen = datas[4];
    		IKAnalyzer analyzer = new IKAnalyzer(true);
    		try {
    			TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(toFen));
    			tokenStream.addAttribute(CharTermAttribute.class);
    			while (tokenStream.incrementToken()) {
    				CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
    				String input = charTermAttribute.toString();
    				System.out.print(input + "|");
                    word.set(input);
                    context.write(word, one);
    			}
    		} catch (Exception e) {
    			System.err.println(e.getMessage());
    		}
    		analyzer.close();
        }
    }
}
