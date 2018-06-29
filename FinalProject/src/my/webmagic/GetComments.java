package my.webmagic;  
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;

import us.codecraft.webmagic.Page;  
import us.codecraft.webmagic.Request;  
import us.codecraft.webmagic.Site;  
import us.codecraft.webmagic.Spider;  
import us.codecraft.webmagic.pipeline.ConsolePipeline;  
import us.codecraft.webmagic.pipeline.JsonFilePipeline;  
import us.codecraft.webmagic.processor.PageProcessor;  
import us.codecraft.webmagic.scheduler.QueueScheduler;  

public class GetComments implements PageProcessor {  
    // 对爬取站点的一些属性进行设置，例如：设置域名，设置代理等；  
    private Site site = Site.me()
    		.setDomain("rate.tmall.com")
    		.setSleepTime(2000)
    	    .addHeader("referer", "https://detail.tmall.com/item.htm?spm=a211oj.11500538/N.5994288170.1.4ad11798yfeETW&acm=ak-zebra-8332-28113.1003.1.2518123&id=562099309982&scm=1003.1.ak-zebra-8332-28113.ITEM_562099309982_2518123&sku_properties=10004:709990523;5919063:6536025")  
    	    .setUserAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.3"); 
    public Site getSite() {  
        return site;  
    }  
    public void process(Page page) {
    	String text = page.getRawText().replace("\"rateDetail\":", "");
    	Map map = JSON.parseObject(text);
        if (map.get("rateList") == null) return;
        Matcher itemIdMatcher = Pattern.compile("itemId=\\d+").matcher(page.getRequest().getUrl());
        String itemIdString = null;
        if (itemIdMatcher.find()) itemIdString = itemIdMatcher.group().replace("itemId=", "");
        Matcher shopIdMatcher = Pattern.compile("sellerId=\\d+").matcher(page.getRequest().getUrl());
        String shopIdString = null;
        if (shopIdMatcher.find()) shopIdString = shopIdMatcher.group().replace("sellerId=", "");
        Matcher currentPageMatcher = Pattern.compile("currentPage=\\d+").matcher(page.getRequest().getUrl());
        String currentPageString = null;
        if (currentPageMatcher.find()) currentPageString = currentPageMatcher.group().replace("currentPage=", "");
        map.put("currentPage",currentPageString);
        map.put("itemId", itemIdString);
        map.put("sellerId", shopIdString);
        map.put("url", page.getRequest().getUrl());
        page.putField("rateDetail", map);
    }  
    public static void main(String[] args) {  
String url_init    = "https://rate.tmall.com/list_detail_rate.htm?" + "itemId=562099309982&sellerId=2616970884&currentPage=1";  

String url_pattern = "https://rate.tmall.com/list_detail_rate.htm?" + "itemId=562099309982&sellerId=2616970884&currentPage=";

        String output = "/home/hduser/tmp/";  
        QueueScheduler scheduler = new QueueScheduler();  
        Spider spider = Spider.create(new GetComments()).addUrl(url_init)  
                .setScheduler(scheduler)  
                .addPipeline(new JsonFilePipeline(output))  
                .addPipeline(new ConsolePipeline());  
        for (int i = 1; i < 100; i++) {  
            Request request = new Request();  
            request.setUrl(url_pattern + i);  
            scheduler.push(request, spider);  
        }  
        spider.thread(50).run();  
    }  
}  
