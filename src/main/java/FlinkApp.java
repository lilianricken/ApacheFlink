import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkApp {
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> stInteger = see.fromElements(1,1,1,2,2,2,3,3,3,4);

        stInteger.countWindowAll(3)
                .reduce(new AggregationFunction<Integer>() {
            public Integer reduce(Integer integer, Integer t1) {
                return integer + t1;
            }
        })
                .map(new MapFunction<Integer, Integer>() {
            public Integer map(Integer integer) {
                return integer*2;
            }
        })
                .filter(new FilterFunction<Integer>() {
                    public boolean filter(Integer integer) throws Exception {
                        return integer > 10;
                    }
                })
                .print();

        see.execute();
    }
}
