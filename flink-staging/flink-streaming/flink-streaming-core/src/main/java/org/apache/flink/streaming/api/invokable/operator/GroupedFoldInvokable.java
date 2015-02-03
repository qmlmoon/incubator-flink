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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.function.FoldFunction;

public class GroupedFoldInvokable<IN, OUT> extends StreamFoldInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;

	private KeySelector<IN, ?> keySelector;
	private Map<Object, OUT> values;
	private OUT initial;
	private TypeSerializer<OUT> outTypeSerializer;

	public GroupedFoldInvokable(OUT initial, FoldFunction<IN, OUT> folder,
								KeySelector<IN, ?> keySelector,
								TypeInformation<OUT> outTypeInformation) {
		super(initial, folder);
		this.initial = initial;
		this.keySelector = keySelector;
		this.outTypeSerializer = outTypeInformation.createSerializer();
		values = new HashMap<Object, OUT>();
	}

	@Override
	protected void fold() throws Exception {
		Object key = nextRecord.getKey(keySelector);
		currentValue = values.get(key);
		if (currentValue == null) {
			currentValue = outTypeSerializer.copy(initial);
		}
		callUserFunctionAndLogException();
		values.put(key, currentValue);
	}

}
