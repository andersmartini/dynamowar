import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;



import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.ServiceAbbreviations;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.s3.model.Region;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;



/**
 * Created by martini on 2016-03-22.
 */
public class DataBase {
    private com.amazonaws.regions.Region currentRegion;

    private AmazonDynamoDBClient client;
    private  DynamoDB dynamoDB;
    private  String movieTableName = "Movies";

    private Region currenRegion;
    private static Table movieTable;
    static Table getMovieTable(){
        return movieTable;
    }



    private static String readUrl(String urlString) throws IOException {
        URL url = new URL(urlString);
        BufferedReader reader = null;

        try {
            HttpURLConnection huc = (HttpURLConnection) url.openConnection();
            HttpURLConnection.setFollowRedirects(false);
            huc.setConnectTimeout(100);
            huc.setRequestMethod("GET");
            huc.setRequestProperty("User-Agent", "anders");
            huc.connect();
            InputStream input = huc.getInputStream();

            reader = new BufferedReader(new InputStreamReader(input, "UTF-8"));
            StringBuilder builder = new StringBuilder();
            for (String line; (line = reader.readLine()) != null; ) {
                builder.append(line).append("\n");
            }
            return builder.toString();
        } finally {
            if (reader != null)
                try {
                    reader.close();
                } catch (IOException ignore) {

                }
        }

    }



    com.amazonaws.regions.Region getCurrentRegion() {
        if (currentRegion == null) {
            if(System.getenv("zone_name") != null){
                try {
                    String zone = readUrl("us-west1a");
                    //zone will be like eu-west1a and then substring -> eu-west, to upper-> EU-WEST and
                    //then replaced -> EU_WEST which corresponds to the enum value
                    currentRegion = RegionUtils.getRegion(
                            zone.substring(0, zone.length() - 2).toUpperCase().replace("-", "_")
                    );
                    return currentRegion;
                } catch (IOException e) {
                    throw new RuntimeException("Server is an ec2 instance but could not get current zone!", e);
                }
            }
        }
        return currentRegion;
    }

    void Connect(){
        if(System.getenv("zone_name")!=null){
            client = new AmazonDynamoDBClient()
                    .withEndpoint(
                            getCurrentRegion().getServiceEndpoint(ServiceAbbreviations.Dynamodb)
                    );

        }else{
            client = new AmazonDynamoDBClient()
                    .withEndpoint("http://localhost:8000");
        }
        dynamoDB = new DynamoDB(client);


    }


    void CreateTable(){
        try {
            System.out.println("Attempting to create table; please wait...");
            movieTable = dynamoDB.createTable(movieTableName,
                    Arrays.asList(
                            new KeySchemaElement("year", KeyType.HASH),  //Partition key
                            new KeySchemaElement("title", KeyType.RANGE)), //Sort key
                    Arrays.asList(
                            new AttributeDefinition("year", ScalarAttributeType.N),
                            new AttributeDefinition("title", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));
            movieTable.waitForActive();
            System.out.println("Success.  Table status: " + movieTable.getDescription().getTableStatus());


        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
        movieTable = dynamoDB.getTable(movieTableName);
    }


    void importData() throws IOException{
        CreateTable();
        URL url = new URL("https://api.themoviedb.org/3/movie/top_rated?api_key=6f1253d21e479061f0b2e708d1cd3cca");
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("GET");
        JsonFactory f = new JsonFactory();

        //try-with-resource
        try (JsonParser parser = f.createParser ( connection.getContent().toString() ) ){

        JsonNode rootNode = new ObjectMapper().readTree(parser);
        Iterator<JsonNode> iter = rootNode.get("results").iterator();
        ObjectNode currentNode;

        while (iter.hasNext()) {
            currentNode = (ObjectNode) iter.next();

            int year = currentNode.path("year").asInt();
            String title = currentNode.path("title").asText();

            try {
                movieTable.putItem(new Item()
                        .withPrimaryKey("year", year, "title", title).withJSON("info", currentNode.path("info").toString())
                );
                System.out.println("putitem succeeded: " + year + " " + title);
            } catch (Exception e) {
                System.err.println("unable to add movie: " + year + "" + title);
                System.err.println(e.getMessage());
                break;
            }
        }
        }catch(Exception e){
            System.err.println("couldn't import data: " + e.getMessage() );
        }
    }
    void drop() throws InterruptedException {
        Table table = getMovieTable();
        table.delete();
        table.waitForDelete();
        System.out.println("Dropped table");
    }


    static class Factory{
        private static DataBase dataBase;

        static DataBase GetDataBase () {
            if(dataBase !=null){
                return dataBase;
            }
            dataBase = new DataBase();
            dataBase.Connect();
            return dataBase;
        }
    }
}
