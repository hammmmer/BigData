package my.mr;
import java.io.IOException;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import com.alibaba.fastjson.JSONArray;  
import com.alibaba.fastjson.JSONObject;  

public class QingXiJson {  
	public static void main(String[] args) throws IOException,  
	    ClassNotFoundException, InterruptedException {  
		Job job = Job.getInstance();  
		job.setJobName("QingXiJson");  
		job.setJarByClass(QingXiJson.class);  
		
		job.setMapperClass(doMapper.class);  
		//job.setReducerClass(doReducer.class);  
		job.setOutputKeyClass(Text.class);  
		job.setOutputValueClass(Text.class);  
		Path in = new Path("hdfs://master:9000/qingxi/in");  
		Path out = new Path("hdfs://master:9000/qingxi/out/1");  
		FileInputFormat.addInputPath(job, in);  
		FileOutputFormat.setOutputPath(job, out);  
		System.exit(job.waitForCompletion(true) ? 0 : 1);  
	}   
	public static class doMapper extends Mapper<Object, Text, Text, Text> {  
	    @Override  
	    protected void map(Object key, Text value, Context context)  
	            throws IOException, InterruptedException {  
	        String initJsonString = value.toString();  
	        JSONObject initJson = JSONObject.parseObject(initJsonString );  
	        if (!initJsonString.contains("rateCount") && !initJsonString.contains("rateContent")) {  
	            return;  
	        }  
	  
	        JSONObject myjson = initJson.getJSONObject("rateDetail");  
	        String itemId = myjson.getString("itemId").toString();
	        JSONObject rateCount = myjson.getJSONObject("rateCount");
	        String rateTotalCount = rateCount.getString("total").toString();

	        /* rateList 包括20条评论 */  
	        JSONArray comments = myjson.getJSONArray("rateList");  
	        for (int i = 0; i < comments.size(); i++) {  
	            JSONObject comment    = comments.getJSONObject(i);  
	            String auctionSku           = comment.getString("auctionSku");  
	            String rateContent        = comment.getString("rateContent").replace('\n', ' ');  
	            String rateDate   = comment.getString("rateDate");  
	            String id          = comment.getString("id");  
	            String displayUserNick       = comment.getString("displayUserNick");  
	            String tradeEndTime  = comment.getString("tradeEndTime");  
	            String userVipLevel = comment.getString("userVipLevel");  
		        String sellerId = comment.getString("sellerId");
	            StringBuilder sb = new StringBuilder();  
	  
	            sb.append(itemId);         sb.append("\t");  
	            sb.append(rateTotalCount);      sb.append("\t");  
	            sb.append( id );            sb.append("\t");  
	            sb.append( auctionSku );         sb.append("\t");  
	            sb.append( rateContent );    sb.append("\t");  
	            sb.append( rateDate );           sb.append("\t");  
	            sb.append( displayUserNick );        sb.append("\t");  
	            sb.append( tradeEndTime );   sb.append("\t");  
	            sb.append( userVipLevel );  sb.append("\t");  
	            sb.append( sellerId );  
	            String result = sb.toString();  
	            context.write(new Text(result), new Text(""));  
	        }  
	    }  
	}   
  
    public static class doReducer extends Reducer<Text, Text, Text, Text>{  
        @Override  
        protected void reduce(Text key, Iterable<Text> values, Context context)  
    	throws IOException, InterruptedException {  
    	}  
    }  
}  