package storm.starter;

import org.apache.storm.starter.bolt.LatencyConfig;
import org.apache.storm.starter.bolt.operation.Operations;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Created by anshushukla on 28/02/17.
 */

public class fooXMLParser extends OurStatefulBoltByteArrayTuple<String,List<byte[]>> {

    String inputFileString=null;
    String name;
//    KeyValueState<String, List<Object>> kvState;
    long sum;
//    public static String traceVal;
    fooXMLParser(String name) {
        this.name = name;
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        initLogger(LoggerFactory.getLogger("APP"));
        l.info("TEST:prepare");
        this.collector = collector;
        _context=context;

//        xml file specific code
                String inputXMLpath="/home/anshu/data/storm/dataset/tempSAX.xml";
//        String inputXMLpath="/Users/anshushukla/Downloads/Storm/storm-1.0.3/examples/storm-starter/src/jvm/org/apache/storm/starter/bolt/operation/tempSAX.xml";

        try {
            inputFileString= LatencyConfig.readFileWithSize(inputXMLpath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    @Override
    public void execute(Tuple input) {
        if(!preExecute(input)){
            return;
            }

        // user code
//        traceVal+=name;
//        l.info("TEST_traceVal_"+traceVal);
//        String upVal= (input.getValueByField("value")).toString()+name;
        String upVal= (input.getValueByField("value")).toString();
//        l.info("TEST_OurStatefulBolt_execute:"+upVal);

        int tot_length = 0;
//        long startTime = System.nanoTime();

        for(int i=0;i<3;i++)
            tot_length += Operations.doXMLparseOp(inputFileString);

//        redisTuples.add(input.getValueByField("MSGID").toString().getBytes(Charset.forName("UTF-8")));
//        l.info("Added_tuples_to_redisTuples_"+input.getValueByField("MSGID"));
//        kvstate.put("redisTuples", redisTuples);
        Values out=  new Values(upVal,input.getValueByField("MSGID").toString());
//        Utils.sleep(2000);
//        emit(input,out);
        collector.emit(out);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value","MSGID"));
    }


//    @Override
//    public void initState(KeyValueState<String, List<Tuple>> state) {
//         List<Tuple> ourOutTuples= (List<Tuple>) state.get("OUR_OUT_TUPLES");
//         List<Tuple> ourPendingTuples= (List<Tuple>) state.get("OUR_PENDING_TUPLES");
//        l.info("TEST: restored tuples from redis ourOutTuples:"+ourOutTuples.size()+"ourPendingTuples:"+ourPendingTuples.size());
//        commitFlag=false;
//        for (Tuple tuple : ourPendingTuples) {
//            execute(tuple);
//        }
//        ourPendingTuples.clear();
//        for (Tuple tuple : ourOutTuples) {
//                    collector.emit(tuple);
//                    collector.ack(tuple);// FIXME: in case we receive the processed tuple
//        }
//        ourOutTuples.clear();
//    }

}

