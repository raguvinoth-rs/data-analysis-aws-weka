/** Raguvinoth R S
**/

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.csvreader.CsvWriter;


public class Scanningdynamo {
	
	static AmazonDynamoDBClient dynamoDB;
	
    // total number of sample items 
    static int scanItemCount = 5955;
    
    // number of items each scan request should return
    static int scanItemLimit =100;
    
    // number of logical segments for parallel scan
    static int parallelScanThreads =60;

    private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\Administrator\\.aws\\credentials).
         */
    
    	AWSCredentials credentials = null;
        
    	try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\Administrator\\.aws\\credentials), and is in valid format.",
                    e);
        }
      
    	dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDB.setRegion(usWest2);}
	
    
    
    public static void main(String[] args) throws Exception {
    	init();
    	
        try {
        	String tableName = "cloud-database-table";           
            
            // Scan the table using multiple threads
            parallelScan(tableName, scanItemLimit, parallelScanThreads);
        }  
        catch (AmazonServiceException ase) {
            System.err.println(ase.getMessage());
        }  
    }
    private static void parallelScan(String tableName, int itemLimit, int scanThreads) {
        System.out.println("Scanning " + tableName + " using " + scanThreads + " threads " + itemLimit + " items at a time");
        ExecutorService executor = Executors.newFixedThreadPool(scanThreads);
        
        // Divide DynamoDB table into logical segments
        // Create one task for scanning each segment
        // Each thread will be scanning one segment
        int totalSeg = scanThreads;
        for (int segment = 0; segment < totalSeg; segment++) {
            // Runnable task that will only scan one segment
            ScanSegmentTask scanTask = new ScanSegmentTask(tableName, itemLimit, totalSeg, segment);
            
            // Execute the task
            executor.execute(scanTask);
        }
        shutDownExecutorService(executor); 
    }
    // Runnable task for scanning a single segment of a DynamoDB table
    private static class ScanSegmentTask implements Runnable {
    	
    	
        
        // DynamoDB table to scan
        private String tableName;
        
        // number of items each scan request should return
        private int itemLimit;
        
        // Total number of segments
        // Equals to total number of threads scanning the table in parallel
        private int totalSeg;
        
        // Segment that will be scanned with by this task
        private int seg;
        
        public ScanSegmentTask(String tableName, int itemLimit, int totalSegments, int segment) {
            this.tableName = tableName;
            this.itemLimit = itemLimit;
            this.totalSeg = totalSegments;
            this.seg = segment;
        }
        
        @Override
        public void run() {
        	
        	System.out.println("Scanning " + tableName + " segment " + seg + " out of " + totalSeg + " segments " + itemLimit + " items at a time...");
            Map<String, AttributeValue> exclusiveStartKey = null;
            int totalScannedItemCount = 0;
            int totalScanRequestCount = 0;
            try {
            	
                while(true) {
                    ScanRequest scanRequest = new ScanRequest()
                        .withTableName(tableName)
                        .withLimit(itemLimit)
                        .withExclusiveStartKey(exclusiveStartKey)
                        .withTotalSegments(totalSeg)
                        .withSegment(seg);
                    
                    ScanResult result = dynamoDB.scan(scanRequest);
                    
                    totalScanRequestCount++;
                    totalScannedItemCount += result.getScannedCount();
                    
                    // print items returned from scan request
                    processScanResult(seg, result);
                    
                    exclusiveStartKey = result.getLastEvaluatedKey();
                    if (exclusiveStartKey == null) {
                        break;
                    }
                }
            } catch (AmazonServiceException | IOException ase) {
                System.err.println(ase.getMessage());
            } finally {
                System.out.println("Scanned " + totalScannedItemCount + " items from segment " + seg + " out of " + totalSeg + " of " + tableName + " with " + totalScanRequestCount + " scan requests");
            }
        }
    }
    
    private static void processScanResult(int segment, ScanResult result)throws IOException{
    	String outputFile = "C:\\ScannedOutput.csv";
    	CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');
    	csvOutput.write("id");
    	csvOutput.write("sclass");
		csvOutput.write("agegroup");
		csvOutput.write("sex");
		csvOutput.write("age");
		csvOutput.write("survived");
		csvOutput.endRecord();for (Map<String, AttributeValue> item : result.getItems()) {
            
			printItem(segment, item,csvOutput);
        }
    }
 private static void printItem(int segment, Map<String, AttributeValue> attributeList,CsvWriter csvOutput) throws IOException{
        System.out.print("Segment " + segment + ", ");
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();
            String data = value.getS();
            csvOutput.write(data);
            
        }
        csvOutput.endRecord();
        
 }
    
    private static void shutDownExecutorService(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}