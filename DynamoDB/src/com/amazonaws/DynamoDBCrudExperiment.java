//Salil Mamodiya

package com.amazonaws;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;


public class DynamoDBCrudExperiment {
	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_2).withCredentials(new ProfileCredentialsProvider()).build(); 
    static DynamoDB dynamoDB = new DynamoDB(client);
    static String tableName = "FirstTable";

    public static void main(String[] args) throws IOException {

    	System.out.println("Creating items");
        addItems();        
        System.out.println("Retriving items");
        getItem();
        System.out.println("Updating items");
        updateOneAttribute();
        System.out.println("Updating multiple attributes ");
        updateManyAttributes();
    	System.out.println("Updating existing attributes conditionally...");
    	//updateAttributeConditionally();
    	//System.out.println("deleting iitem...");
    	deleteItem();
    	
        System.out.println("All the operations done");

    }

    private static void addItems() {

        Table table = dynamoDB.getTable(tableName);
        try {
  
            	Item item = new Item().withPrimaryKey("Name", "Salil Mamodiya").withNumber("Age", 22)
                    .withString("Company", "Samsung");
                table.putItem(item);
                
                item = new Item().withPrimaryKey("Name", "Salil Mamodiya").withNumber("Age", 23)
                        .withString("Company", "Samsung").withString("College","Guwahati");
                    table.putItem(item);

                item = new Item().withPrimaryKey("Name", "Mark Zuckeburg").withNumber("Age", 33)
                        .withString("Company", "Facebook");
                    table.putItem(item);
                table.putItem(item);

        }
        catch (Exception e) {
            System.err.println("Item Creation Failed.....");
            System.err.println(e.getMessage());

        }
    }
    
    private static void getItem() {
        Table table = dynamoDB.getTable(tableName);

        try {

            Item item = table.getItem("Name", "Salil Mamodiya", "Age, Company", null);

            System.out.println("Item printing after retrieval...");
            System.out.println(item.toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Failed to get item... ");
            System.err.println(e.getMessage());
        }

    }
    
    private static void updateOneAttribute() {
        Table table = dynamoDB.getTable(tableName);

        try {

            Map<String, String> expressionAttributeNames = new HashMap<String, String>();
            expressionAttributeNames.put("#na", "NewAttribute");

            UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Name", "Salil Mamodiya")
                .withUpdateExpression("set #na = :val1").withNameMap(new NameMap().with("#na", "PreviousCompany"))
                .withValueMap(new ValueMap().withString(":val1", "Xerox Research")).withReturnValues(ReturnValue.ALL_NEW);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            // Check the response.
            System.out.println("Printing item after one attribute has been added...");
            System.out.println(outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Failed to add a attribute in " + tableName);
            System.err.println(e.getMessage());
        }
    }
    
    private static void updateManyAttributes() {
        Table table = dynamoDB.getTable(tableName);

        try {

            UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Name", "Salil Mamodiya")
                .withUpdateExpression("set #a=:val1, #na=:val2")
                .withNameMap(new NameMap().with("#a", "Company").with("#na", "Hometown"))
                .withValueMap(
                    new ValueMap().withString(":val1", "Samsung RandD").withString(":val2", "jaipur"))
                .withReturnValues(ReturnValue.ALL_NEW);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            System.out.println("Printing item after multiple attribute update...");
            System.out.println(outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Failed to update many attributes in " + tableName);
            System.err.println(e.getMessage());

        }
    }
    
    private static void updateAttributeConditionally() {

        Table table = dynamoDB.getTable(tableName);

        try {
            UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Name", "Salil Mamodiya")
                .withReturnValues(ReturnValue.ALL_NEW).withUpdateExpression("set #p = :val1")
                .withConditionExpression("#p = :val2").withNameMap(new NameMap().with("#p", "Age"))
                .withValueMap(new ValueMap().withNumber(":val1", 23).withNumber(":val2", 22));

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            // Check the response.
            System.out.println("Printing item after conditional update to new attribute...");
            System.out.println(outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Error updating item in " + tableName);
            System.err.println(e.getMessage());
        }
    }
    
    private static void deleteItem() {

        Table table = dynamoDB.getTable(tableName);

        try {

            DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey("Name", "Salil Mamodiya")
                .withConditionExpression("#ip = :val").withNameMap(new NameMap().with("#ip", "Age"))
                .withValueMap(new ValueMap().withNumber(":val", 23)).withReturnValues(ReturnValue.ALL_OLD);

            DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);

            // Check the response.
            System.out.println("Printing item that was deleted...");
            System.out.println(outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Error deleting item in " + tableName);
            System.err.println(e.getMessage());
        }
    }
}