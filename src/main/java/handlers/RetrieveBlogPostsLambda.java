package handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class RetrieveBlogPostsLambda implements RequestStreamHandler {

    private static final String DB_URL = System.getenv("DB_URL_KEY");

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) {
        LambdaLogger LOGGER = context.getLogger();
        LOGGER.log("Starting RetrieveBlogPostsLambda\n", LogLevel.INFO);
        JSONParser parser = new JSONParser();
        ObjectMapper mapper = new ObjectMapper();
        APIGatewayV2HTTPResponse responseEvent = new APIGatewayV2HTTPResponse();
        APIGatewayV2HTTPEvent requestEvent;
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Methods", "GET, OPTIONS");
        headers.put("Access-Control-Allow-Headers", "Content-Type");

        Connection connection = null;

        try {
            // Parse the received JSON content into an APIGatewayV2HTTPEvent object
            JSONObject event;
            try {
                event = (JSONObject) parser.parse(new InputStreamReader(input));
            } catch (IOException | ParseException ex) {
                LOGGER.log("Error parsing input into JSON format: " + ex.getMessage(), LogLevel.ERROR);
                throw new RuntimeException(ex);
            }
            try {
                requestEvent = mapper.readValue(event.toJSONString(), APIGatewayV2HTTPEvent.class);
            } catch (JsonProcessingException ex) {
                LOGGER.log("Error parsing APIGatewayV2HTTPEvent: " + ex.getMessage(), LogLevel.ERROR);
                throw new RuntimeException(ex);
            }
            LOGGER.log("Parsed APIGatewayV2HTTPEvent: " + requestEvent, LogLevel.DEBUG);

            // Return http status = 200 OK if request method is OPTIONS
            if (requestEvent.getHeaders() != null && requestEvent.getHeaders().containsKey("httpMethod") &&
                    "OPTIONS".equals(requestEvent.getHeaders().get("httpMethod"))) {
                responseEvent.setStatusCode(200);
                responseEvent.setHeaders(headers);
                responseEvent.setIsBase64Encoded(false);
                try {
                    mapper.writeValue(output, responseEvent);
                } catch (IOException ex) {
                    LOGGER.log("Error processing pre-flight options request: " + ex.getMessage(), LogLevel.ERROR);
                    throw new RuntimeException(ex);
                }
                return;
            }

            try {
                LOGGER.log("Getting DB credentials...\n");

                SecretsManagerClient client = SecretsManagerClient.builder()
                        .region(Region.US_EAST_2)
                        .build();

                GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                        .secretId(System.getenv("SECRET_NAME"))
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

                // Create results set object
                ResultSet resultSet;

                // Check for query parameters and perform search if present
                if (requestEvent.getQueryStringParameters() != null) {
                    try {
                        LOGGER.log("Query string parameters: " + requestEvent.getQueryStringParameters() + "\n");

                        Map<String, String> queryStringParameters = requestEvent.getQueryStringParameters();
                        String searchTerm = queryStringParameters.getOrDefault("searchTerm", null);
                        String page = queryStringParameters.getOrDefault("page", "1");
                        String limit = queryStringParameters.getOrDefault("limit", "5");
                        String order = queryStringParameters.getOrDefault("order", "d");
                        String searchTags = queryStringParameters.getOrDefault("searchTags", null);
                        String searchKeywords = queryStringParameters.getOrDefault("searchKeywords", null);

                        // Generate PostgreSQL query based on query parameters
                        StringBuilder query = new StringBuilder("SELECT * FROM blog_page.blog_post WHERE 1 = 1");

                        if (searchTerm != null) {
                            searchTerm = searchTerm.replace(" ", "%");
                            query.append(" AND content ILIKE ?");
                        }
                        if (searchTags != null) {
                            query.append(" AND tags @> ARRAY[?]::text[]");
                        }
                        if (searchKeywords != null) {
                            query.append(" AND keywords @> ARRAY[?]::text[]");
                        }
                        if (order != null) {
                            query.append(" ORDER BY ?");
                        }
                        if (limit != null) {
                            query.append(" LIMIT ?");
                        }
                        if (page != null) {
                            query.append(" OFFSET ?");
                        }

                        LOGGER.log("Generated query: " + query + "\n");

                        // Prepare and execute the query
                        PreparedStatement preparedStatement = connection.prepareStatement(query.toString());

                        int parameterIndex = 1;
                        if (searchTerm != null) {
                            preparedStatement.setString(parameterIndex++, "%" + searchTerm + "%");
                        }
                        if (searchTags != null) {
                            preparedStatement.setString(parameterIndex++, searchTags);
                        }
                        if (searchKeywords != null) {
                            preparedStatement.setString(parameterIndex++, searchKeywords);
                        }
                        if (order != null) {
                            if ("a".equals(order)) {
                                order = "ASC";
                            } else {
                                order = "DESC";
                            }
                            preparedStatement.setString(parameterIndex++, order);
                        }
                        if (limit != null) {
                            preparedStatement.setInt(parameterIndex++, Integer.parseInt(limit));
                        }
                        if (page != null) {
                            preparedStatement.setInt(parameterIndex, (Integer.parseInt(page) - 1) * Integer.parseInt(limit));
                        }
                        LOGGER.log("Prepared statement: " + preparedStatement + "\n", LogLevel.DEBUG);
                        resultSet = preparedStatement.executeQuery();

                    } catch (Exception e) {
                        LOGGER.log("Exception: " + e.getMessage());
                        // Prepare error response
                        String errorResponse = "{\"error\": \"An error occurred while processing the search request.\"}";
                        responseEvent.setStatusCode(500); // Internal Server Error
                        responseEvent.setHeaders(headers);
                        responseEvent.setBody(errorResponse);
                        responseEvent.setIsBase64Encoded(false);
                        return;
                    }
                } else {
                    // Fetch last 5 blog posts from the DB based on ID
                    LOGGER.log("No query parameters found\n", LogLevel.DEBUG);
                    Statement statement = connection.createStatement();
                    resultSet = statement.executeQuery("SELECT * FROM blog_page.blog_post ORDER BY id DESC LIMIT 5;");
                }

                // Construct JSON array
                ArrayNode blogPostsArray = mapper.createArrayNode();

                if (resultSet != null) {
                    while (resultSet.next()) {
                        String postTitle = resultSet.getString("title");
                        String postContent = resultSet.getString("content");

                        // Create the JSON object for the post
                        ObjectNode postNode = mapper.createObjectNode();
                        postNode.put("title", postTitle);
                        postNode.put("content", postContent);

                        blogPostsArray.add(postNode);
                    }
                } else {
                    ObjectNode postNode = mapper.createObjectNode();
                    postNode.put("title", "No Results Found");
                    postNode.put("content", "Please rephrase your search term and try again.");

                    blogPostsArray.add(postNode);
                }

                // Prepare response
                String responseBody = mapper.writeValueAsString(blogPostsArray);
                LOGGER.log("Response: " + responseBody, LogLevel.INFO);
                responseEvent = new APIGatewayV2HTTPResponse();
                responseEvent.setStatusCode(200);
                responseEvent.setHeaders(headers);
                responseEvent.setBody(responseBody);
                responseEvent.setIsBase64Encoded(false);

            } catch (ClassNotFoundException | SQLException e) {
                LOGGER.log("Exception: " + e.getMessage(), LogLevel.ERROR);
                // Prepare error response
                String errorResponse = "{\"error\": \"An error occurred while processing the request.\"}";
                responseEvent = new APIGatewayV2HTTPResponse();
                responseEvent.setStatusCode(500); // Internal Server Error
                responseEvent.setHeaders(headers);
                responseEvent.setBody(errorResponse);
                responseEvent.setIsBase64Encoded(false);

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

            // Write response to output stream
            try (OutputStreamWriter writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
                writer.write(mapper.writeValueAsString(responseEvent));
            } catch (IOException e) {
                LOGGER.log("Error writing response: " + e.getMessage(), LogLevel.ERROR);

            }
        } catch ( IOException e) {
            LOGGER.log("Error reading input: " + e.getMessage(), LogLevel.ERROR);
            throw new RuntimeException(e);
        }
    }
}


