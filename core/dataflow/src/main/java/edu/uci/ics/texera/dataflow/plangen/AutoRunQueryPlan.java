package edu.uci.ics.texera.dataflow.plangen;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

import java.io.IOException;
import java.util.*;

/**
 * A class to handle auto run query plan request .
 *
 * @author Ming Huang
 */

public class AutoRunQueryPlan {

    private String logicalPlanJson;

    public AutoRunQueryPlan(String logicalPlanJson) {
        this.logicalPlanJson = logicalPlanJson;
    }

    public JsonNode run() throws IOException{
        JsonNode logicalPlanNode = new ObjectMapper().readTree(logicalPlanJson);
        ArrayNode operators = (ArrayNode) logicalPlanNode.get(PropertyNameConstants.OPERATOR_LIST);
        ArrayNode links = (ArrayNode) logicalPlanNode.get(PropertyNameConstants.OPERATOR_LINK_LIST);

        ArrayNode validOperators = new ObjectMapper().createArrayNode();
        ArrayNode validLinks = new ObjectMapper().createArrayNode();
        ArrayNode linksEndWithInvalidDest = new ObjectMapper().createArrayNode();

        Set<String> validOperatorsId = new HashSet<>();
        getValidOperatorsAndLinks(operators, links, validOperators, validLinks,
                linksEndWithInvalidDest, validOperatorsId);

        ((ObjectNode) logicalPlanNode).putArray(PropertyNameConstants.OPERATOR_LIST).addAll(validOperators);
        ((ObjectNode) logicalPlanNode).putArray(PropertyNameConstants.OPERATOR_LINK_LIST).addAll(validLinks);

        LogicalPlan logicalPlan = new ObjectMapper().treeToValue(logicalPlanNode, LogicalPlan.class);
        String resultID = UUID.randomUUID().toString();

        // Get all input schema for valid operator with valid links
        Map<String, List<Schema>> inputSchema = logicalPlan.retrieveAllOperatorInputSchema();
        // Get all input schema for invalid operator with valid input operator
        for (JsonNode linkNode: linksEndWithInvalidDest) {
            String origin = linkNode.get(PropertyNameConstants.ORIGIN_OPERATOR_ID).textValue();
            String dest = linkNode.get(PropertyNameConstants.DESTINATION_OPERATOR_ID).textValue();

            Schema schema = logicalPlan.getOperatorOutputSchema(origin, inputSchema);
            inputSchema.computeIfAbsent(dest, k -> new ArrayList<>()).add(schema);
        }

        ObjectNode result = new ObjectMapper().createObjectNode();
        for (Map.Entry<String, List<Schema>> entry: inputSchema.entrySet()) {
            Set<String> attributes = new HashSet<>();
            for (Schema schema: entry.getValue()) {
                attributes.addAll(schema.getAttributeNames());
            }

            ArrayNode currentSchemaNode = result.putArray(entry.getKey());
            for (String attrName: attributes) {
                currentSchemaNode.add(attrName);
            }

        }

        ObjectNode response = new ObjectMapper().createObjectNode();
        response.put("code", 0);
        response.set("result", result);
        response.put("resultID", resultID);
        return response;
    }

    private void getValidOperatorsAndLinks(ArrayNode operators, ArrayNode links,
                                           ArrayNode validOperators, ArrayNode validLinks,
                                           ArrayNode linksEndWithInvalidDest, Set<String> validOperatorsId) {
        // Try to convert to valid operator
        for (JsonNode operatorNode: operators) {
            try {
                new ObjectMapper().treeToValue(operatorNode, PredicateBase.class);
                validOperators.add(operatorNode);
                validOperatorsId.add(operatorNode.get(PropertyNameConstants.OPERATOR_ID).textValue());
                // Fail
            } catch (JsonProcessingException e) {
                System.out.println(e);
            }
        }

        // Only include edges that connect valid operators
        for (JsonNode linkNode: links) {
            String origin = linkNode.get(PropertyNameConstants.ORIGIN_OPERATOR_ID).textValue();
            String dest = linkNode.get(PropertyNameConstants.DESTINATION_OPERATOR_ID).textValue();

            if (validOperatorsId.contains(origin) && validOperatorsId.contains(dest)) {
                validLinks.add(linkNode);
            } else if (validOperatorsId.contains(origin) && !validOperatorsId.contains(dest)) {
                linksEndWithInvalidDest.add(linkNode);
            }
        }
    }

}
