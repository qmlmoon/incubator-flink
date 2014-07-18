/**
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
package org.apache.flink.languagebinding.api.java.streaming;

import java.io.Serializable;

/**
 * Implementations of this class are used to manipulate data before sending/retrieving it to/from other processes.
 * Refer to the conversion tables in the respective Streamer classes for more information on which types can be streamed
 * directly.
 * @param <IN>
 * @param <OUT>
 */
public abstract class Converter<IN, OUT> implements Serializable {
	public abstract OUT convert(IN value);
}
