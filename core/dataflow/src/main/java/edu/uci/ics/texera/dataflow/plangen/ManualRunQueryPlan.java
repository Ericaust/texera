package edu.uci.ics.texera.dataflow.plangen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.engine.Engine;
import edu.uci.ics.texera.api.engine.Plan;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A class to handle manual run query plan request .
 *
 * @author Ming Huang
 */

public class ManualRunQueryPlan {

    private static java.nio.file.Path resultDirectory = Utils.getTexeraHomePath().resolve("query-results");
    private String logicalPlanJson;

    public ManualRunQueryPlan(String logicalPlanJson) {
        this.logicalPlanJson = logicalPlanJson;
    }

    public JsonNode run() throws IOException, TexeraException {
        LogicalPlan logicalPlan = new ObjectMapper().readValue(logicalPlanJson, LogicalPlan.class);
        Plan plan = logicalPlan.buildQueryPlan();
        ISink sink = plan.getRoot();

        // send response back to frontend
        if (sink instanceof TupleSink) {
            TupleSink tupleSink = (TupleSink) sink;
            tupleSink.open();
            List<Tuple> results = tupleSink.collectAllTuples();
            tupleSink.close();

            // make sure result directory is created
            if (Files.notExists(resultDirectory)) {
                Files.createDirectories(resultDirectory);
            }

            // clean up old result files
            cleanupOldResults();

            // generate new UUID as the result id
            String resultID = UUID.randomUUID().toString();

            // write original json of the result into a file
            java.nio.file.Path resultFile = resultDirectory.resolve(resultID + ".json");

            Files.createFile(resultFile);
            Files.write(resultFile, new ObjectMapper().writeValueAsBytes(results));

            // put readable json of the result into response
            ArrayNode resultNode = new ObjectMapper().createArrayNode();
            for (Tuple tuple : results) {
                resultNode.add(tuple.getReadableJson());
            }

            ObjectNode response = new ObjectMapper().createObjectNode();
            response.put("code", 0);
            response.set("result",resultNode);
            response.put("resultID", resultID);
            return response;
        } else {
            // execute the plan and return success message
            Engine.getEngine().evaluate(plan);
            ObjectNode response = new ObjectMapper().createObjectNode();
            response.put("code", 1);
            response.put("message", "plan sucessfully executed");
            return response;
        }
    }

    /**
     * Cleans up the old result json files stored in the file system.
     * The current cleanup policy is to keep the latest 5 files.
     *
     * TODO: In the case where there are multiple users, they need their own spaces for storing files.
     *
     * @throws IOException
     */
    private static void cleanupOldResults() throws IOException {
        // list all the files in the reuslt directory
        List<java.nio.file.Path> resultFiles = Files.list(resultDirectory).collect(Collectors.toList());

        // clean up if there are more than 5 files
        if (resultFiles.size() <= 5) {
            return;
        }

        // sort the files by their creation time
        Collections.sort(resultFiles, (java.nio.file.Path f1, java.nio.file.Path f2) -> {
            try {
                return (
                        Files.readAttributes(f1, BasicFileAttributes.class).creationTime().compareTo(
                                Files.readAttributes(f2, BasicFileAttributes.class).creationTime()));
            } catch (IOException e) {
                return 0;
            }
        });

        // remove the oldest file
        java.nio.file.Path oldestFile = resultFiles.get(0);
        Files.delete(oldestFile);
    }
}
