package org.pucpr;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class FlinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> file = see.readTextFile("src\\main\\resources\\ocorrencias_criminais.csv");

        SingleOutputStreamOperator<Crime> crimeMap = file.map((String t) -> {
            String[] columns = t.split(";");
            Integer dia = Integer.parseInt(columns[0]);
            Integer mes = Integer.parseInt(columns[1]);
            Integer ano = Integer.parseInt(columns[2]);
            String tipo = columns[4];
            return new Crime(dia, mes, ano, tipo);
        });

        AllWindowedStream<Crime, GlobalWindow> crimeMapTousand = crimeMap.countWindowAll(10000);


        //1. A cada 10 mil crimes a quantidade de crimes do tipo NARCOTICS;
        crimeMapTousand.aggregate(new AggregateFunction<Crime, Integer, Integer>() {
            public Integer createAccumulator() {
                return 0;
            }

            public Integer add(Crime crime, Integer acc) {
                if (crime.tipo.contains("NARCOTICS")) {
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
                .print("NARCOTICS");

        //2. A cada 10 mil crimes, a quantidade de crimes do tipo NARCOTICS que ocorreram no ano 2010
        crimeMapTousand.aggregate(new AggregateFunction<Crime, Integer, Integer>() {
            public Integer createAccumulator() {
                return 0;
            }

            public Integer add(Crime crime, Integer acc) {
                if (crime.tipo.contains("NARCOTICS") && crime.ano.equals(2010)) {
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
                .print("NARCOTICS em 2010");

        //3.A cada 10 mil crimes, a quantidade de crimes que ocorreram no dia 1;
        crimeMapTousand.aggregate(new AggregateFunction<Crime, Integer, Integer>() {
            public Integer createAccumulator() {
                return 0;
            }

            public Integer add(Crime crime, Integer acc) {
                if (crime.dia.equals(1)) {
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
                .print("Crimes no dia 01");

        //4.A cada 10 mil crimes, a quantidade de crimes que ocorreram no dia 1 e que sejam do tipo NARCOTICS;
        crimeMapTousand.aggregate(new AggregateFunction<Crime, Integer, Integer>() {
            public Integer createAccumulator() {
                return 0;
            }

            public Integer add(Crime crime, Integer acc) {
                if (crime.tipo.contains("NARCOTICS") && crime.dia.equals(1)) {
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
                .print("NARCOTICS no 01");

        //5.A cada 10 mil crimes, a quantidade de crimes que ocorreram no dia 1, que sejam do tipo NARCOTICS.
        // Do resultado gerado obter uma média;
        crimeMapTousand.aggregate(new AggregateFunction<Crime, Integer, Tuple3<String, Integer, Integer>>() {
            public Integer createAccumulator() {
                return 0;
            }

            public Integer add(Crime crime, Integer acc) {
                if (crime.tipo.contains("NARCOTICS") && crime.dia.equals(1)) {
                    return acc + 1;
                }
                return acc;
            }

            public Tuple3<String, Integer, Integer> getResult(Integer acc) {
                return new Tuple3<>("Narcotics", acc, 1);
            }

            public Integer merge(Integer acc, Integer acc1) {
                return acc + acc1;
            }
        })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> t2, Tuple3<String, Integer, Integer> t1) {
                        return new Tuple3<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<String, Integer, Integer>, Integer>() {
                    @Override
                    public Integer map(Tuple3<String, Integer, Integer> t) {
                        return t.f1 / t.f2;
                    }
                })
                .print("Média de NARCOTICS no 01");

        //6.A cada 10 mil crimes, a quantidade de crimes que ocorreram no dia 1, que sejam do tipo NARCOTICS.
        // Do resultado gerado obter o valor máximo;
        crimeMapTousand.aggregate(new AggregateFunction<Crime, Integer, Tuple2<String, Integer>>() {
            public Integer createAccumulator() {
                return 0;
            }

            public Integer add(Crime crime, Integer acc) {
                if (crime.tipo.contains("NARCOTICS") && crime.dia.equals(1)) {
                    return acc + 1;
                }
                return acc;
            }

            public Tuple2<String, Integer> getResult(Integer acc) {
                return new Tuple2<>("Narcotics", acc);
            }

            public Integer merge(Integer acc, Integer acc1) {
                return acc + acc1;
            }
        })
                .keyBy(0)
                .max(1)
                .print("Maior ocorrência de NARCOTICS no 01");

       //7.Para crimes do tipo NARCOTICS. A cada 10 mil crimes ocorridos no dia 1.
        // Agrupar os crimes de acordo com o mês. Gerar uma soma com os resultados obtidos.
        crimeMap.filter(new FilterFunction<Crime>() {
            @Override
            public boolean filter(Crime crime) throws Exception {
                return crime.tipo.contains("NARCOTICS");
            }
        }).keyBy("mes")
                .countWindowAll(10000)
                .apply(new AllWindowFunction<Crime, Tuple2<Integer, Integer>, GlobalWindow>() {
                    @Override
                    public void apply(GlobalWindow window, Iterable<Crime> values, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        int key = 0;
                        int count = 0;
                        for (Crime c : values) {
                            if (c.dia == 1) {
                                key = c.mes;
                                count++;
                            }
                        }
                        out.collect(new Tuple2<>(key, count));
                    }
                })
                .keyBy(0)
                .sum(1)
                .print("Soma Narcotics dia 01 por mês");
        see.execute();
    }

    public static class Crime implements Serializable {
        public Integer dia;
        public Integer mes;
        public Integer ano;
        public String tipo;

        public Crime(Integer dia, Integer mes, Integer ano, String tipo) {
            this.dia = dia;
            this.mes = mes;
            this.ano = ano;
            this.tipo = tipo;
        }
    }
}
