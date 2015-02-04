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
package org.apache.flink.compiler.dag;

import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.base.CoGroupPythonOperatorBase;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.operators.CoGroupPythonDescriptor;
import org.apache.flink.compiler.operators.OperatorDescriptorDual;

/**
 * The Optimizer representation of a <i>PythonCoGroup</i> operator.
 */
public class CoGroupPythonNode extends TwoInputNode {
	private List<OperatorDescriptorDual> dataProperties;

	public CoGroupPythonNode(CoGroupPythonOperatorBase<?, ?, ?, ?> pactContract) {
		super(pactContract);
		this.dataProperties = initializeDataProperties();
	}

	// --------------------------------------------------------------------------------------------
	/**
	 * Gets the operator for this CoGroup node.
	 * 
	 * @return The CoGroup operator.
	 */
	@Override
	public CoGroupPythonOperatorBase<?, ?, ?, ?> getPactContract() {
		return (CoGroupPythonOperatorBase<?, ?, ?, ?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "CoGroup";
	}

	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		return this.dataProperties;
	}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// for CoGroup, we currently make no reasonable default estimates
	}

	private List<OperatorDescriptorDual> initializeDataProperties() {
		Ordering groupOrder1 = null;
		Ordering groupOrder2 = null;

		CoGroupPythonOperatorBase<?, ?, ?, ?> cgc = getPactContract();
		groupOrder1 = cgc.getGroupOrderForInputOne();
		groupOrder2 = cgc.getGroupOrderForInputTwo();

		if (groupOrder1 != null && groupOrder1.getNumberOfFields() == 0) {
			groupOrder1 = null;
		}
		if (groupOrder2 != null && groupOrder2.getNumberOfFields() == 0) {
			groupOrder2 = null;
		}

		return Collections.<OperatorDescriptorDual>singletonList(new CoGroupPythonDescriptor(this.keys1, this.keys2, groupOrder1, groupOrder2));
	}
}