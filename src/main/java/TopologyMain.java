import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;


public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {

        //Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Yahoo-Finance-Spout", new yfSpout());
        builder.setBolt("Yahoo-Finance-Bolt", new yfBolt())
                .shuffleGrouping("Yahoo-Finance-Spout");

        StormTopology topology = builder.createTopology();
        //Configuration
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToWrite", "C:\\Users\\09242\\Documents\\output.txt");

        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("Local-Yahoo-Finance-Topology", conf, topology);
            Thread.sleep(3000);
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            cluster.shutdown();
        }
    }


}
