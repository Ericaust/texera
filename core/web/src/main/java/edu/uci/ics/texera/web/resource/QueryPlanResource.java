package edu.uci.ics.texera.web.resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.engine.Engine;
import edu.uci.ics.texera.api.engine.Plan;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.plangen.AutoRunQueryPlan;
import edu.uci.ics.texera.dataflow.plangen.LogicalPlan;
import edu.uci.ics.texera.dataflow.plangen.ManualRunQueryPlan;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.web.TexeraWebException;

/**
 * This class will be the resource class for accepting a query plan edu.uci.ics.texera.web.request and executing the
 * query plan to get the query response
 * Created by kishorenarendran on 10/17/16.
 * 
 * @author Kishore
 * @author Zuozhi
 */
@Path("/queryplan")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryPlanResource {
    
    public static java.nio.file.Path resultDirectory = Utils.getTexeraHomePath().resolve("query-results");
    
    /**
     * This is the edu.uci.ics.texera.web.request handler for the execution of a Query Plan.
     * @param logicalPlanJson, the json representation of the logical plan
     * @return - Generic TexeraWebResponse object
     */
    @POST
    @Path("/execute")
    // TODO: investigate how to use LogicalPlan directly
    public JsonNode executeQueryPlan(String logicalPlanJson) {
        ManualRunQueryPlan manualRunQueryPlan = new ManualRunQueryPlan(logicalPlanJson);
        try {
            return manualRunQueryPlan.run();
        } catch (IOException | TexeraException e) {
            throw new TexeraWebException(e.getMessage());
        }   
    }

    /**
     * This is the edu.uci.ics.texera.web.request handler for the execution of a Query Plan.
     * @param logicalPlanJson, the json representation of the logical plan
     * @return - Generic TexeraWebResponse object
     */
    /* EG of using /autocomplete end point (how this inline update method works):

    1. At the beginning of creating a graph, (for example) when a scan source and a keyword search
        operators are initailized (dragged in the flow-chart) but unlinked, the graph looks like this:

         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: N/A  |
        |  TableName: N/A   |                           |  (Other inputs...)|
        |                   |                           |                   |
        |                   |                           |                   |
        |                   |                           |                   |
        |___________________|                           |___________________|

    2. Then, you can feel free to link these two operators together, or go ahead and select a
        table as the source first. Let's link them together first.

         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: N/A  |
        |  TableName: N/A   | ========================> |  (Other inputs...)|
        |                   |                           |                   |
        |                   |                           |                   |
        |                   |                           |                   |
        |___________________|                           |___________________|

    3. At this moment, the Keyword Search operator still does NOT have any available options for
        its Attributes field because of the lack of the source. Therefore, we can select a table
        name as the source next (let's use table "plan" as an example here)

         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: N/A  |
        |  TableName: plan  | ========================> |  (Other inputs...)|
        |                   |                           |                   |
        |                   |                           |                   |
        |                   |                           |                   |
        |___________________|                           |___________________|

    4. After select table "plan" as the source, now you can see the options list of Attributes in
        the Keyword Search operator becomes available. you should see 4 options in the list: name,
        description, logicPlan, payload. Feel free to choose whichever you need for your desired result.

         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: name |
        |  TableName: plan  | ========================> |  (Other inputs...)|
        |                   |                           |                   |
        |                   |                           |                   |
        |                   |                           |                   |
        |___________________|                           |___________________|

    5. Basically, the method supports that whenever you connect a source (with a valid table name)
        to a regular search operator, the later operator is able to recognize the metadata of its
        input operator (which is the source), and then updates its attribute options in the drop-down
        list. To illustrate how powerful this functionality is, you can add a new (Scan) Source and
        pick another table which is different than table "plan" we have already created. The graph
        now should be looked like the following:

         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: name |
        |  TableName: plan  | ========================> |  (Other inputs...)|
        |                   |                           |                   |
        |                   |                           |                   |
        |                   |                           |                   |
        |___________________|                           |___________________|


         _______________________
        |                       |
        |                       |
        | Source: Scan          |
        | TableName: dictionary |
        |                       |
        |                       |
        |                       |
        |_______________________|

    6. Then, connect "dictionary" to the Keyword Search operator. The original link between "plan"
        and Keyword Search will automatically disappear.


         ___________________                             ___________________
        |                   |                           |                   |
        |                   |                           |  Keyword Search   |
        |  Source: Scan     |                           |  Attributes: N/A  |
        |  TableName: plan  |                           |  (Other inputs...)|
        |                   |            /============> |                   |
        |                   |           //              |                   |
        |                   |          //               |                   |
        |___________________|         //                |___________________|
                                     //
                                    //
         _______________________   //
        |                       | //
        |                       |//
        | Source: Scan          |/
        | TableName: dictionary |
        |                       |
        |                       |
        |                       |
        |_______________________|

    7. After the new link generated, the Attributes field of the Keyword Search will be empty again. When
        you try to check its drop-down list, the options are all updated to dictionary's attributes, which
        are name and payload. The options from "plan" are all gone.

    */
    @POST
    @Path("/autocomplete")
    public JsonNode executeAutoQueryPlan(String logicalPlanJson) {
        AutoRunQueryPlan autoRunQueryPlan = new AutoRunQueryPlan(logicalPlanJson);
        try {
            return autoRunQueryPlan.run();
        } catch (JsonMappingException je) {
            ObjectNode response = new ObjectMapper().createObjectNode();
            response.put("code", -1);
            response.put("message", "Json Mapping Exception would not be handled for auto plan. " + je.getMessage());
            return response;

        } catch (IOException | TexeraException e) {
            if (e.getMessage().contains("does not exist in the schema:")) {
                ObjectNode response = new ObjectMapper().createObjectNode();
                response.put("code", -1);
                response.put("message", "Attribute Not Exist Exception would not be handled for auto plan. " + e.getMessage());
                return response;
            }
            throw new TexeraWebException(e.getMessage());
        }
    }
    
    





}
