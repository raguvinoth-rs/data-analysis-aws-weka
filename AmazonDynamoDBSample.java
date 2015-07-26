/** Raguvinoth R S 
**/

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

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
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class AmazonDynamoDBSample {


    static AmazonDynamoDBClient dynamoDB;

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
        dynamoDB.setRegion(usWest2);
    }

    public static void main(String[] args) throws Exception {
        init();

        try {
            String tableName = "cloud-database-table";

            // Create table if it does not exist yet
            if (Tables.doesTableExist(dynamoDB, tableName)) {
                System.out.println("Table " + tableName + " is already ACTIVE");
            } else {
                // Create a table with a primary hash key named 'name', which holds a string
            	CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                        .withKeySchema(new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH))
                        .withAttributeDefinitions(new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.S))
                        .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
                        TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
                System.out.println("Created Table: " + createdTableDescription);

                // Wait for it to become active
                System.out.println("Waiting for " + tableName + " to become ACTIVE...");
                Tables.waitForTableToBecomeActive(dynamoDB, tableName);
            }

            /*
            // Describe our new table
            DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
            TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
            System.out.println("Table Description: " + tableDescription);
            */
            
            // Add an item
            
            String inpFile ="C:/Users/Administrator/Desktop/workspace/Cloudproj4/cldinp.csv";
        	BufferedReader br = null;
        	String line = "";
        	String splitBy = ",";
         
        	try {
         
        		br = new BufferedReader(new FileReader(inpFile));
        		while ((line = br.readLine()) != null) {
         
        		        // use comma as separator
        			String[] cloud = line.split(splitBy);
        	        
        			        			
                    Map<String, AttributeValue> item = newItem(cloud[0],cloud[1],cloud[2],cloud[3],cloud[4],cloud[5]);
                    PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
                    PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
                    System.out.println("Result: " + putItemResult);

            
            
            /*

            // Add another item
            item = newItem("Airplane", 1980, "*****", "James", "Billy Bob");
            putItemRequest = new PutItemRequest(tableName, item);
            putItemResult = dynamoDB.putItem(putItemRequest);
            System.out.println("Result: " + putItemResult);

            // Scan items for movies with a year attribute greater than 1985
            HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
            Condition condition = new Condition()
                .withComparisonOperator(ComparisonOperator.GT.toString())
                .withAttributeValueList(new AttributeValue().withN("1985"));
            scanFilter.put("year", condition);
            ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
            ScanResult scanResult = dynamoDB.scan(scanRequest);
            System.out.println("Result: " + scanResult);
*/
        		}
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        }
        	} catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
   
    
    private static Map<String, AttributeValue> newItem(String id, String sclass, String agegroup, String sex, String age, String survived) 
    {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("id", new AttributeValue(id));
        item.put("sclass", new AttributeValue(sclass));
        item.put("agegroup", new AttributeValue(agegroup));
        item.put("sex", new AttributeValue(sex));
        item.put("age", new AttributeValue(age));
        item.put("survived", new AttributeValue(survived));
        
        return item;
    }

}