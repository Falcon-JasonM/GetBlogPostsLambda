package handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RetrieveBlogPostsLambda implements RequestStreamHandler {

    private static final String DB_URL = System.getenv("DB_URL_KEY");

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {

        LambdaLogger LOGGER = context.getLogger();
        ObjectMapper mapper = new ObjectMapper();
        Connection connection = null;
        final String secretName = System.getenv("SECRET_NAME");
        ArrayList<String> blogPosts = new ArrayList<>();

        // Cast the input as an APIGatewayProxyRequestEvent to handle HTTP GET requests
        APIGatewayProxyRequestEvent requestEvent = new ObjectMapper().readValue(input, APIGatewayProxyRequestEvent.class);

        if ("GET".equals(requestEvent.getHttpMethod())) {
            
            try {
                LOGGER.log("Getting DB credentials...\n");

                SecretsManagerClient client = SecretsManagerClient.builder()
                        .region(Region.US_EAST_2)
                        .build();

                GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                        .secretId(secretName)
                        .build();

                GetSecretValueResponse getSecretValueResponse = client.getSecretValue(getSecretValueRequest);

                String secret = getSecretValueResponse.secretString();
                Map<String, String> secretMap = mapper.readValue(secret, Map.class);
                String dbUsername = secretMap.get("username");
                String dbPassword = secretMap.get("password");
                LOGGER.log("Got DB credentials\n", LogLevel.DEBUG);

                LOGGER.log("Attempting to connect to the DB...\n", LogLevel.INFO);

                // Load the PostgreSQL JDBC driver
                Class.forName("org.postgresql.Driver");
                connection = DriverManager.getConnection(DB_URL, dbUsername, dbPassword);
                LOGGER.log("Connected to the DB\n", LogLevel.DEBUG);

                // Fetch blog posts from the database
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM BlogPosts");

                while (resultSet.next()) {
                    String postTitle = resultSet.getString("title");
                    String postContent = resultSet.getString("content");
                    // Use JSON object structure to represent each post
                    String post = "{\"title\":\"" + postTitle + "\",\"content\":\"" + postContent + "\"}";
                    blogPosts.add(post);
                }

                // Construct JSON array
                ArrayNode blogPostsArray = mapper.createArrayNode();
                for (String post : blogPosts) {
                    blogPostsArray.add(mapper.readTree(post));
                }

                // Prepare response
                String responseBody = mapper.writeValueAsString(blogPostsArray);
                LOGGER.log("Response: " + responseBody, LogLevel.INFO);

                // Set headers
                Map<String, String> headers = new HashMap<>();
                headers.put("Content-Type", "application/json");
                // Prepare http response to the API Gateway
                APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withHeaders(headers)
                        .withBody(responseBody);
                // Write response to output stream
                try (OutputStreamWriter writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
                    writer.write(responseEvent.toString());
                } catch (IOException e) {
                    LOGGER.log("Error writing response: " + e.getMessage(), LogLevel.ERROR);
                }

            } catch (ClassNotFoundException | SQLException e) {
                LOGGER.log("Exception: " + e.getMessage(), LogLevel.ERROR);
                // Prepare error response
                String errorResponse = "{\"error\": \"An error occurred while processing the request.\"}";
                Map<String, String> headers = new HashMap<>();
                headers.put("Content-Type", "application/json");
                try (OutputStreamWriter writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
                    writer.write(errorResponse);
                } catch (IOException ioException) {
                    LOGGER.log("Error writing error response: " + ioException.getMessage(), LogLevel.ERROR);
                }
            } finally {
                // Close resources
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    LOGGER.log("Error closing connection: " + e.getMessage(), LogLevel.ERROR);
                }
            }
        }
    }
}
