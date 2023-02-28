/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


import java.io.DataOutputStream;
import java.io.File;
import java.util.Collection;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		//1、创建flink环境
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//2、绑定数据源
		DataStreamSource<String> source = env.readTextFile("D:\\data\\code\\back\\java\\flink\\flink-batch\\src\\main\\resources\\sourceFile.txt");
		//3、逐行读取数据
		SingleOutputStreamOperator<Tuple2<String,Long>> result = source.flatMap((String line, Collector<Tuple2<String,Long>> out) -> {
			String[] words = line.split(" |,");
			for(String word:words){
				System.out.print(word);
				out.collect(Tuple2.of(word,1L));
			}
		}).returns(Types.TUPLE(Types.STRING,Types.LONG));//指定返回类型
		//4、按照单词分组统计
		KeyedStream streamData = result.keyBy(data -> data.f0);
		SingleOutputStreamOperator<Tuple2<String,Long>> sumResult = streamData.sum(1);
		sumResult.print();
//		DataStream groupResult = result.keyBy((KeySelector) o -> { //
//			return o;
//		});
//
//		groupResult.print();

		env.execute();
	}
}
