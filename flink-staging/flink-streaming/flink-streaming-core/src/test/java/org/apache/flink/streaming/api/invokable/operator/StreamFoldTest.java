/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.function.FoldFunction;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class StreamFoldTest {

	private static class MyFolder implements FoldFunction<Integer, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String fold(String start, Integer value2) throws Exception {
			return start + value2.toString();
		}

	}

	@Test
	public void test() {
		StreamFoldInvokable<Integer, String> invokable1 = new StreamFoldInvokable<Integer, String>( "",
			new MyFolder());

		List<String> expected = Arrays.asList("1","11","112","1123","11233");
		List<String> actual = MockContext.createAndExecute(invokable1,
			Arrays.asList(1, 1, 2, 3, 3));

		assertEquals(expected, actual);
	}

}
