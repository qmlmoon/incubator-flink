/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * ********************************************************************************************************************/
package eu.stratosphere.languagebinding.api.java.python.functions;

import com.google.protobuf.InvalidProtocolBufferException;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.typeutils.ResultTypeQueryable;
import static eu.stratosphere.api.java.typeutils.TypeExtractor.getForObject;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.languagebinding.api.java.python.streaming.PythonStreamer;
import eu.stratosphere.languagebinding.api.java.streaming.Converter;
import eu.stratosphere.types.TypeInformation;
import eu.stratosphere.util.Collector;
import java.io.IOException;

/**
 FlatMap-function that uses an external python function.
 * @param <IN> 
 * @param <OUT> 
 */
public class PythonFlatMap<IN, OUT> extends FlatMapFunction<IN, OUT> implements ResultTypeQueryable {
	private final String operator;
	private PythonStreamer streamer;
	private Converter inConverter;
	private Converter outConverter;
	private final Object typeInformation;
	private String metaInformation;

	public PythonFlatMap(String operator, Object typeInformation) {
		this.operator = operator;
		this.typeInformation = typeInformation;
	}

	public PythonFlatMap(String operator, Object typeInformation, String metaInformation) {
		this(operator, typeInformation);
		this.metaInformation = metaInformation;
	}

	public PythonFlatMap(String operator, Object typeInformation,
			Converter inConverter, Converter outConverter) {
		this(operator, typeInformation);
		this.inConverter = inConverter;
		this.outConverter = outConverter;
	}

	/**
	 Opens this function.
	 @param ignored ignored
	 @throws IOException 
	 */
	@Override
	public void open(Configuration ignored) throws IOException {
		streamer = inConverter == null && outConverter == null
				? new PythonStreamer(this, operator, metaInformation)
				: new PythonStreamer(this, operator, inConverter, outConverter);
		streamer.open();
	}

	/**
	 * Call the external python function.
	 * @param value function input
	 * @param collector
	 * @throws Exception 
	 */
	@Override
	public final void flatMap(IN value, Collector<OUT> collector) throws Exception {
		try {
			streamer.stream(value, collector);
		} catch (InvalidProtocolBufferException ipbe) {
			throw new IOException("An error occurred while receiving data. This usually means that the python process "
					+ "has prematurely terminated (or may have never started)", ipbe);
		} catch (IOException ioe) {
			if (ioe.getMessage().startsWith("Stream closed")) {
				throw new IOException(
						"The python process has prematurely terminated (or may have never started).", ioe);
			}
			throw ioe;
		}
	}

	/**
	 Closes this function.
	 @throws IOException 
	 */
	@Override
	public void close() throws IOException {
		streamer.close();
	}

	@Override
	public TypeInformation getProducedType() {
		return getForObject(typeInformation);
	}
}