/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;

import java.text.ParseException;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;

/**
 * @author sandeepreddy602
 *
 */
public class ScanBasedSourceOperatorTest {
    
    private LuceneDataWriter dataWriter;
    private ScanBasedSourceOperator scanBasedSourceOperator;
    private IDataReader dataReader;
    
    @Before
    public void setUp() throws Exception{
        dataWriter = new LuceneDataWriter(LuceneDataStore.DATA_STORE_DIRECTORY);
        dataWriter.clearData();
        dataWriter.writeData(TestConstants.SAMPLE_SCHEMA_PEOPLE, TestConstants.getSamplePeopleTuples());
        dataReader = new LuceneDataReader(
                LuceneDataStore.DATA_STORE_DIRECTORY, TestConstants.SAMPLE_SCHEMA_PEOPLE, 
                LuceneConstants.SCAN_QUERY, TestConstants.SAMPLE_SCHEMA_PEOPLE.get(0).getFieldName());
        scanBasedSourceOperator = new ScanBasedSourceOperator(dataReader);
    }
    
    @After
    public void cleanUp() throws Exception{
        dataWriter.clearData();
    }
    
    @Test
    public void testFlow() throws DataFlowException, ParseException{
        List<ITuple> tuples = TestConstants.getSamplePeopleTuples();
        scanBasedSourceOperator.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        while((nextTuple  = scanBasedSourceOperator.getNextTuple()) != null){
            //Checking if the tuple retrieved is present in the samplesTuples
            boolean contains = TestUtils.contains(tuples, nextTuple);
            Assert.assertTrue(contains);
            numTuples ++;
        }
        Assert.assertEquals(tuples.size(), numTuples);
        scanBasedSourceOperator.close();
    }
    
}
