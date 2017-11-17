/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.genevents.factory.ArgumentClass;
import org.apache.storm.starter.genevents.factory.ArgumentParser;
import org.apache.storm.starter.spout.SampleSpoutWithCHKPTSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example topology that demonstrates the use of {@link org.apache.storm.topology.IStatefulBolt}
 * to manage state. To run the example,
 * <pre>
 * $ storm jar examples/storm-starter/storm-starter-topologies-*.jar storm.starter.StatefulTopology statetopology
 * </pre>
 * <p/>
 * The default state used is 'InMemoryKeyValueState' which does not persist the state across restarts. You could use
 * 'RedisKeyValueState' to test state persistence by setting below property in conf/storm.yaml
 * <pre>
 * topology.state.provider: org.apache.storm.redis.state.RedisKeyValueStateProvider
 * </pre>
 * <p/>
 * You should also start a local redis instance before running the 'storm jar' command. The default
 * RedisKeyValueStateProvider parameters can be overridden in conf/storm.yaml, for e.g.
 * <p/>
 * <pre>
 * topology.state.provider.config: '{"keyClass":"...", "valueClass":"...",
 *                                   "keySerializerClass":"...", "valueSerializerClass":"...",
 *                                   "jedisPoolConfig":{"host":"localhost", "port":6379,
 *                                      "timeout":2000, "database":0, "password":"xyz"}}'
 *
 * </pre>
 * </p>
 */
public class FooLinearParseTopology100 {
    private static final Logger LOG = LoggerFactory.getLogger(FooLinearParseTopology100.class);


    public static void main(String[] args) throws Exception {

        /** Common Code begins **/
        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }


        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;

        String taskPropFilename=argumentClass.getTasksPropertiesFilename();
        System.out.println("taskPropFilename-"+taskPropFilename);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SampleSpoutWithCHKPTSpout(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()), 1);

        //        builder.setSpout("spout", new OurRandomIntegerWithCHKPTSpout());
//        builder.setSpout("spout", new fooRandomIntegerWithCHKPTSpout());

        builder.setBolt("fooPartial2", new fooXMLParser("2"), 1).shuffleGrouping("spout","datastream");
        builder.setBolt("fooPartial3", new fooXMLParser("3"), 1).shuffleGrouping("fooPartial2");
        builder.setBolt("fooPartial4", new fooXMLParser("4"), 1).shuffleGrouping("fooPartial3");
        builder.setBolt("fooPartial5", new fooXMLParser("5"), 1).shuffleGrouping("fooPartial4");
        builder.setBolt("fooPartial6", new fooXMLParser("6"), 1).shuffleGrouping("fooPartial5");
        builder.setBolt("fooPartial7", new fooXMLParser("7"), 1).shuffleGrouping("fooPartial6");
        builder.setBolt("fooPartial8", new fooXMLParser("8"), 1).shuffleGrouping("fooPartial7");
        builder.setBolt("fooPartial9", new fooXMLParser("9"), 1).shuffleGrouping("fooPartial8");
        builder.setBolt("fooPartial10", new fooXMLParser("10"), 1).shuffleGrouping("fooPartial9");
        builder.setBolt("fooPartial11", new fooXMLParser("11"), 1).shuffleGrouping("fooPartial10");

        builder.setBolt("fooPartial12", new fooXMLParser("12"), 1).shuffleGrouping("fooPartial11");
        builder.setBolt("fooPartial13", new fooXMLParser("13"), 1).shuffleGrouping("fooPartial12");
        builder.setBolt("fooPartial14", new fooXMLParser("14"), 1).shuffleGrouping("fooPartial13");
        builder.setBolt("fooPartial15", new fooXMLParser("15"), 1).shuffleGrouping("fooPartial14");
        builder.setBolt("fooPartial16", new fooXMLParser("16"), 1).shuffleGrouping("fooPartial15");
        builder.setBolt("fooPartial17", new fooXMLParser("17"), 1).shuffleGrouping("fooPartial16");
        builder.setBolt("fooPartial18", new fooXMLParser("18"), 1).shuffleGrouping("fooPartial17");
        builder.setBolt("fooPartial19", new fooXMLParser("19"), 1).shuffleGrouping("fooPartial18");
        builder.setBolt("fooPartial20", new fooXMLParser("20"), 1).shuffleGrouping("fooPartial19");
        builder.setBolt("fooPartial21", new fooXMLParser("21"), 1).shuffleGrouping("fooPartial20");

        builder.setBolt("fooPartial22", new fooXMLParser("22"), 1).shuffleGrouping("fooPartial21");
        builder.setBolt("fooPartial23", new fooXMLParser("23"), 1).shuffleGrouping("fooPartial22");
        builder.setBolt("fooPartial24", new fooXMLParser("24"), 1).shuffleGrouping("fooPartial23");
        builder.setBolt("fooPartial25", new fooXMLParser("25"), 1).shuffleGrouping("fooPartial24");
        builder.setBolt("fooPartial26", new fooXMLParser("26"), 1).shuffleGrouping("fooPartial25");
        builder.setBolt("fooPartial27", new fooXMLParser("27"), 1).shuffleGrouping("fooPartial26");
        builder.setBolt("fooPartial28", new fooXMLParser("28"), 1).shuffleGrouping("fooPartial27");
        builder.setBolt("fooPartial29", new fooXMLParser("29"), 1).shuffleGrouping("fooPartial28");
        builder.setBolt("fooPartial30", new fooXMLParser("30"), 1).shuffleGrouping("fooPartial29");
        builder.setBolt("fooPartial31", new fooXMLParser("31"), 1).shuffleGrouping("fooPartial30");

        builder.setBolt("fooPartial32", new fooXMLParser("32"), 1).shuffleGrouping("fooPartial31");
        builder.setBolt("fooPartial33", new fooXMLParser("33"), 1).shuffleGrouping("fooPartial32");
        builder.setBolt("fooPartial34", new fooXMLParser("34"), 1).shuffleGrouping("fooPartial33");
        builder.setBolt("fooPartial35", new fooXMLParser("35"), 1).shuffleGrouping("fooPartial34");
        builder.setBolt("fooPartial36", new fooXMLParser("36"), 1).shuffleGrouping("fooPartial35");
        builder.setBolt("fooPartial37", new fooXMLParser("37"), 1).shuffleGrouping("fooPartial36");
        builder.setBolt("fooPartial38", new fooXMLParser("38"), 1).shuffleGrouping("fooPartial37");
        builder.setBolt("fooPartial39", new fooXMLParser("39"), 1).shuffleGrouping("fooPartial38");
        builder.setBolt("fooPartial40", new fooXMLParser("40"), 1).shuffleGrouping("fooPartial39");
        builder.setBolt("fooPartial41", new fooXMLParser("41"), 1).shuffleGrouping("fooPartial40");

        builder.setBolt("fooPartial42", new fooXMLParser("42"), 1).shuffleGrouping("fooPartial41");
        builder.setBolt("fooPartial43", new fooXMLParser("43"), 1).shuffleGrouping("fooPartial42");
        builder.setBolt("fooPartial44", new fooXMLParser("44"), 1).shuffleGrouping("fooPartial43");
        builder.setBolt("fooPartial45", new fooXMLParser("45"), 1).shuffleGrouping("fooPartial44");
        builder.setBolt("fooPartial46", new fooXMLParser("46"), 1).shuffleGrouping("fooPartial45");
        builder.setBolt("fooPartial47", new fooXMLParser("47"), 1).shuffleGrouping("fooPartial46");
        builder.setBolt("fooPartial48", new fooXMLParser("48"), 1).shuffleGrouping("fooPartial47");
        builder.setBolt("fooPartial49", new fooXMLParser("49"), 1).shuffleGrouping("fooPartial48");
        builder.setBolt("fooPartial50", new fooXMLParser("50"), 1).shuffleGrouping("fooPartial49");
        builder.setBolt("fooPartial51", new fooXMLParser("51"), 1).shuffleGrouping("fooPartial50");

        builder.setBolt("fooPartial52", new fooXMLParser("52"), 1).shuffleGrouping("fooPartial51");
        builder.setBolt("fooPartial53", new fooXMLParser("53"), 1).shuffleGrouping("fooPartial52");
        builder.setBolt("fooPartial54", new fooXMLParser("54"), 1).shuffleGrouping("fooPartial53");
        builder.setBolt("fooPartial55", new fooXMLParser("55"), 1).shuffleGrouping("fooPartial54");
        builder.setBolt("fooPartial56", new fooXMLParser("56"), 1).shuffleGrouping("fooPartial55");
        builder.setBolt("fooPartial57", new fooXMLParser("57"), 1).shuffleGrouping("fooPartial56");
        builder.setBolt("fooPartial58", new fooXMLParser("58"), 1).shuffleGrouping("fooPartial57");
        builder.setBolt("fooPartial59", new fooXMLParser("59"), 1).shuffleGrouping("fooPartial58");
        builder.setBolt("fooPartial60", new fooXMLParser("60"), 1).shuffleGrouping("fooPartial59");

        builder.setBolt("fooPartial61", new fooXMLParser("61"), 1).shuffleGrouping("fooPartial60");
        builder.setBolt("fooPartial62", new fooXMLParser("62"), 1).shuffleGrouping("fooPartial61");
        builder.setBolt("fooPartial63", new fooXMLParser("63"), 1).shuffleGrouping("fooPartial62");
        builder.setBolt("fooPartial64", new fooXMLParser("64"), 1).shuffleGrouping("fooPartial63");
        builder.setBolt("fooPartial65", new fooXMLParser("65"), 1).shuffleGrouping("fooPartial64");
        builder.setBolt("fooPartial66", new fooXMLParser("66"), 1).shuffleGrouping("fooPartial65");
        builder.setBolt("fooPartial67", new fooXMLParser("67"), 1).shuffleGrouping("fooPartial66");
        builder.setBolt("fooPartial68", new fooXMLParser("68"), 1).shuffleGrouping("fooPartial67");
        builder.setBolt("fooPartial69", new fooXMLParser("69"), 1).shuffleGrouping("fooPartial68");
        builder.setBolt("fooPartial70", new fooXMLParser("70"), 1).shuffleGrouping("fooPartial69");

        builder.setBolt("fooPartial71", new fooXMLParser("71"), 1).shuffleGrouping("fooPartial70");
        builder.setBolt("fooPartial72", new fooXMLParser("72"), 1).shuffleGrouping("fooPartial71");
        builder.setBolt("fooPartial73", new fooXMLParser("73"), 1).shuffleGrouping("fooPartial72");
        builder.setBolt("fooPartial74", new fooXMLParser("74"), 1).shuffleGrouping("fooPartial73");
        builder.setBolt("fooPartial75", new fooXMLParser("75"), 1).shuffleGrouping("fooPartial74");
        builder.setBolt("fooPartial76", new fooXMLParser("76"), 1).shuffleGrouping("fooPartial75");
        builder.setBolt("fooPartial77", new fooXMLParser("77"), 1).shuffleGrouping("fooPartial76");
        builder.setBolt("fooPartial78", new fooXMLParser("78"), 1).shuffleGrouping("fooPartial77");
        builder.setBolt("fooPartial79", new fooXMLParser("79"), 1).shuffleGrouping("fooPartial78");
        builder.setBolt("fooPartial80", new fooXMLParser("80"), 1).shuffleGrouping("fooPartial79");

        builder.setBolt("fooPartial81", new fooXMLParser("81"), 1).shuffleGrouping("fooPartial80");
        builder.setBolt("fooPartial82", new fooXMLParser("82"), 1).shuffleGrouping("fooPartial81");
        builder.setBolt("fooPartial83", new fooXMLParser("83"), 1).shuffleGrouping("fooPartial82");
        builder.setBolt("fooPartial84", new fooXMLParser("84"), 1).shuffleGrouping("fooPartial83");
        builder.setBolt("fooPartial85", new fooXMLParser("85"), 1).shuffleGrouping("fooPartial84");
        builder.setBolt("fooPartial86", new fooXMLParser("86"), 1).shuffleGrouping("fooPartial85");
        builder.setBolt("fooPartial87", new fooXMLParser("87"), 1).shuffleGrouping("fooPartial86");
        builder.setBolt("fooPartial88", new fooXMLParser("88"), 1).shuffleGrouping("fooPartial87");
        builder.setBolt("fooPartial89", new fooXMLParser("89"), 1).shuffleGrouping("fooPartial88");
        builder.setBolt("fooPartial90", new fooXMLParser("90"), 1).shuffleGrouping("fooPartial89");

        builder.setBolt("fooPartial91", new fooXMLParser("91"), 1).shuffleGrouping("fooPartial90");
        builder.setBolt("fooPartial92", new fooXMLParser("92"), 1).shuffleGrouping("fooPartial91");
        builder.setBolt("fooPartial93", new fooXMLParser("93"), 1).shuffleGrouping("fooPartial92");
        builder.setBolt("fooPartial94", new fooXMLParser("94"), 1).shuffleGrouping("fooPartial93");
        builder.setBolt("fooPartial95", new fooXMLParser("95"), 1).shuffleGrouping("fooPartial94");
        builder.setBolt("fooPartial96", new fooXMLParser("96"), 1).shuffleGrouping("fooPartial95");
        builder.setBolt("fooPartial97", new fooXMLParser("97"), 1).shuffleGrouping("fooPartial96");
        builder.setBolt("fooPartial98", new fooXMLParser("98"), 1).shuffleGrouping("fooPartial97");
        builder.setBolt("fooPartial99", new fooXMLParser("99"), 1).shuffleGrouping("fooPartial98");
        builder.setBolt("fooPartial100", new fooXMLParser("100"), 1).shuffleGrouping("fooPartial99");
        builder.setBolt("fooPartial101", new fooXMLParser("101"), 1).shuffleGrouping("fooPartial99");

//        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("fooPartial6");
        builder.setBolt("sink", new fooSink(sinkLogFileName), 1).shuffleGrouping("fooPartial101");


        Config conf = new Config();
        conf.setNumAckers(1);
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE,false);
        conf.put(Config.TOPOLOGY_DEBUG, false);
        conf.put(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL, 10); //FIXME:AS4
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, new Integer(1048576));
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, new Integer(1048576));
        conf.put(Config.TOPOLOGY_STATE_PROVIDER,"org.apache.storm.redis.state.RedisKeyValueStateProvider");

        //        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,30); // in sec.
//        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, new Integer(32));
//        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS,5);

        if (argumentClass.getDeploymentMode().equals("C")) {
//            conf.setNumWorkers(1);
//            conf.setNumWorkers(6);
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, builder.createTopology());
        }

        else {
            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();
            cluster.submitTopology("test", conf, topology);
            Utils.sleep(400000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}


//    L   IdentityTopology   /Users/anshushukla/Downloads/Incomplete/stream/PStormScheduler/src/test/java/operation/output/eventDist.csv     PLUG-210  1.0   /Users/anshushukla/data/output/tempr      /Users/anshushukla/Downloads/Incomplete/stream/iot-bm-For-Scheduler/modules/tasks/src/main/resources/tasks.properties  test
