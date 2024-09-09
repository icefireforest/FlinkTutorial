package org.hss.day1.sample1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用即将过时的DataSet做有界数据处理，后面版本全部转向DataStream处理，
 * 即使有界流也使用DataStream进行处理
 */
public class DataSetWordCount {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> stringDataSource = executionEnvironment.readTextFile("input/wc.txt");
        FlatMapOperator<String, Tuple2<String, Long>> tupleOperator = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] arr = s.split(" ");
                for (String str : arr) {
                    collector.collect(Tuple2.of(str, 1L));
                }
            }
        });
        UnsortedGrouping<Tuple2<String, Long>> tupleGrouping = tupleOperator.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = tupleGrouping.sum(1);
        sum.print();

    }
}
