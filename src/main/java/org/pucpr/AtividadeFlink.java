package org.pucpr;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AtividadeFlink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> file = see.readTextFile("src\\main\\resources\\ocorrencias_criminais.csv");

        file.countWindowAll(10000).aggregate(new AggregateFunction<String, Integer, Integer>() {
            public Integer createAccumulator() {
                return 0;
            }

            public Integer add(String s, Integer acc) {
                String[] columns = s.split(";");
                String tipo = columns[4];
                String ano = columns[2];
                if (tipo.contains("NARCOTICS") && ano.contains("2010")) {
                    return acc + 1;
                }
                return acc;
            }

            public Integer getResult(Integer acc) {
                return acc;
            }

            public Integer merge(Integer acc, Integer acc1) {
                return acc + acc1;
            }
        })
                .print();

        see.execute();
    }
}
