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

package org.apache.flink.streaming.api.function;

import org.apache.flink.api.common.functions.AbstractRichFunction;

/**
 * A FoldFunction represents a Fold transformation.
 * In addition to that the user can use the features provided by the
 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
 *
 * @param <IN>
 *            Type of the input data stream.
 * @param <OUT>
 *            Type of the initial value as well as the output.
 */
public abstract class RichFoldFunction<IN, OUT> extends AbstractRichFunction implements FoldFunction<IN, OUT> {

	private static final long serialVersionUID = 1L;

}
