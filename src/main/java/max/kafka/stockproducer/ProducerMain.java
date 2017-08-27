package max.kafka.stockproducer;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
//import org.jsoup.Jsoup;
//import org.jsoup.nodes.Document;
//import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 *
 */
public class ProducerMain 
{
	private static final Logger LOG = LoggerFactory.getLogger(ProducerMain.class);
	
    public static void main( String[] args ) throws Exception
    {  	
    	File f = new File("MI_5MINS_INDEX.csv");
    	List<String> lines = FileUtils.readLines(f, Charset.defaultCharset());
    	
    	//Remove title and header
    	lines.remove(0);
    	lines.remove(0);
    	
    	KafkaProducerService kafkaSrv = new KafkaProducerServiceImpl();
    	kafkaSrv.initProducer();
    	
    	try {
	    	for (String line: lines) {
	    		//Formating the data
	    		line = StringUtils.replace(
	        			StringUtils.substringBeforeLast(line, ",")
	        			, "=", ""); 
	    		
	    		String[] splits = line.split("\",\"");
	    		if (splits.length < 3) {continue;}
	    		//LOG.info(line);
	    		String time = generateFakeTime(splits[0].replace("\"", ""));
	    		String price = splits[1].replace(",", "");
	    		String value = time + "_" + price;
	    			    		
	    		//Send through Kafka	    		    		
	    		kafkaSrv.send("test", value);
	    		
	    		//Thread.sleep(5000);
	    	}
    	} finally {
    		kafkaSrv.stopProducer();
    	}
    	
    }
    
    public static String generateFakeTime(String time) {
    	String[] splits = StringUtils.split(time, ":");
    	
    	try {
    		Calendar cal = Calendar.getInstance();
    		cal.set(Calendar.HOUR_OF_DAY, Integer.valueOf(splits[0]));
    		cal.set(Calendar.MINUTE, Integer.valueOf(splits[1]));
    		cal.set(Calendar.SECOND, Integer.valueOf(splits[2]));
    		
    		Date date = cal.getTime();
    		long timeL = date.getTime();
    		
    		//LOG.info("date: {}", date);
    		//LOG.info("timeL: {}", timeL);
    		
			return String.valueOf(timeL);
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
		}
    	
    	return null;
    }
}
