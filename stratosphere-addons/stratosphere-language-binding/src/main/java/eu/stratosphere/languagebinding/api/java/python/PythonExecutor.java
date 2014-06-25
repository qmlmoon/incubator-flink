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
package eu.stratosphere.languagebinding.api.java.python;

import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.io.CsvInputFormat;
import eu.stratosphere.api.java.io.CsvOutputFormat;
import eu.stratosphere.api.java.io.PrintingOutputFormat;
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.api.java.io.TextOutputFormat;
import eu.stratosphere.api.java.io.jdbc.JDBCInputFormat;
import eu.stratosphere.api.java.io.jdbc.JDBCOutputFormat;
import eu.stratosphere.api.java.operators.CrossOperator.CrossProjection;
import eu.stratosphere.api.java.operators.CrossOperator.DefaultCross;
import eu.stratosphere.api.java.operators.CrossOperator.ProjectCross;
import eu.stratosphere.api.java.operators.Grouping;
import eu.stratosphere.api.java.operators.JoinOperator.DefaultJoin;
import eu.stratosphere.api.java.operators.JoinOperator.JoinProjection;
import eu.stratosphere.api.java.operators.JoinOperator.ProjectJoin;
import eu.stratosphere.api.java.operators.ProjectOperator;
import eu.stratosphere.api.java.operators.ProjectOperator.Projection;
import eu.stratosphere.api.java.operators.SingleInputUdfOperator;
import eu.stratosphere.api.java.operators.SortedGrouping;
import eu.stratosphere.api.java.operators.TwoInputUdfOperator;
import eu.stratosphere.api.java.operators.UnsortedGrouping;
import eu.stratosphere.api.java.tuple.Tuple;
import static eu.stratosphere.api.java.typeutils.BasicTypeInfo.STRING_TYPE_INFO;
import static eu.stratosphere.api.java.typeutils.TypeExtractor.getForObject;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.languagebinding.api.java.proto.streaming.ProtoReceiver;
import eu.stratosphere.languagebinding.api.java.proto.streaming.ProtoSender;
//CHECKSTYLE.OFF: AvoidStarImport - enum/function import
import static eu.stratosphere.languagebinding.api.java.python.PythonExecutor.OperationInfo.*;
import eu.stratosphere.languagebinding.api.java.python.functions.*;
//CHECKSTYLE.ON: AvoidStarImport
import static eu.stratosphere.languagebinding.api.java.streaming.Receiver.createTuple;
import eu.stratosphere.languagebinding.api.java.streaming.StreamPrinter;
import eu.stratosphere.pact.runtime.cache.FileCache;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;

/**
 This class allows the execution of a stratosphere plan written in python.
 */
public class PythonExecutor {
	private static ProtoSender sender;
	private static ProtoReceiver receiver;
	private static Process process;
	private static HashMap<Integer, Object> sets;
	private static ExecutionEnvironment env;
	private static String tmpPackagePath;
	private static String tmpPlanPath;

	public static final byte BYTE = new Integer(1).byteValue();
	public static final int SHORT = new Integer(1).shortValue();
	public static final int INT = 1;
	public static final long LONG = 1L;
	public static final String STRING = "type";
	public static final double FLOAT = 1.5F;
	public static final double DOUBLE = 1.5D;
	public static final boolean BOOLEAN = true;

	private static final boolean LOCAL = false;

	public static String STRATOSPHERE_HDFS_PATH = LOCAL ? "/tmp/stratosphere" : "hdfs:/tmp/stratosphere";
	public static String STRATOSPHERE_PYTHON_HDFS_PATH = STRATOSPHERE_HDFS_PATH + "/stratosphere";
	public static String STRATOSPHERE_GOOGLE_HDFS_PATH = STRATOSPHERE_HDFS_PATH + "/stratosphere/google";
	public static String STRATOSPHERE_EXECUTOR_HDFS_PATH = STRATOSPHERE_HDFS_PATH + "/stratosphere/executor.py";

	public static final String STRATOSPHERE_PYTHON_ID = "stratosphere";
	public static final String STRATOSPHERE_GOOGLE_ID = "google";
	public static final String STRATOSPHERE_EXECUTOR_ID = "executor";
	public static final String STRATOSPHERE_USER_ID = "userpackage";
	public static final String STRATOSPHERE_PLAN_ID = "plan";

	public static String STRATOSPHERE_PYTHON_PATH_PREFIX = LOCAL
			? new File("src/main/python/eu/stratosphere/languagebinding/api/python").getAbsolutePath()
			: "/resources/python";
	public static String STRATOSPHERE_PYTHON_PATH_SUFFIX = STRATOSPHERE_PYTHON_PATH_PREFIX + "/stratosphere";
	public static String STRATOSPHERE_GOOGLE_PATH_SUFFIX = STRATOSPHERE_PYTHON_PATH_PREFIX + "/google";
	public static String STRATOSPHERE_EXECUTOR_PATH_SUFFIX = STRATOSPHERE_PYTHON_PATH_PREFIX + "/executor.py";

	public static String STRATOSPHERE_DIR = LOCAL ? "" : System.getenv("STRATOSPHERE_ROOT_DIR");

	/**
	 Entry point for the execution of a python plan.
	 @param args 
	 [0] = local path to user package
	 [1] = local path to python script containing the plan
	 [X] = additional parameters passed to the plan
	 @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		try {
			env = ExecutionEnvironment.getExecutionEnvironment();
			distributePackages(args[1], args[0]);
			open(Arrays.copyOfRange(args, 2, args.length));
			receivePlan();
			env.execute();
			close();
		} catch (Exception ex) {
			try {
				close();
			} catch (NullPointerException npe) {
			}
			throw ex;
		}
	}

	private static void open(String[] args) throws IOException {
		sets = new HashMap();
		process = Runtime.getRuntime().exec("python " + tmpPlanPath, args);
		sender = new ProtoSender(null, process.getOutputStream());
		receiver = new ProtoReceiver(null, process.getInputStream());
		new StreamPrinter(process.getErrorStream()).start();
	}

	private static void distributePackages(String planPath, String packagePath) throws IOException, URISyntaxException {
		ensureConditions(planPath, packagePath);
		//move user package to the same folder as executor (for pythonpath reasons)
		if (packagePath.endsWith("/")) {
			packagePath = packagePath.substring(0, packagePath.length() - 1);
		}
		tmpPackagePath = STRATOSPHERE_DIR + STRATOSPHERE_PYTHON_PATH_PREFIX
				+ packagePath.substring(packagePath.lastIndexOf("/"));
		FileCache.copy(new Path(packagePath), new Path(tmpPackagePath), false);

		//move user plan to the same folder as executor (for pythonpath reasons)
		if (planPath.endsWith("/")) {
			planPath = planPath.substring(0, planPath.length() - 1);
		}
		tmpPlanPath = STRATOSPHERE_DIR + STRATOSPHERE_PYTHON_PATH_PREFIX
				+ planPath.substring(planPath.lastIndexOf("/"));
		FileCache.copy(new Path(planPath), new Path(tmpPlanPath), false);

		//copy stratosphere package to hdfs
		FileCache.copy(
				new Path(STRATOSPHERE_DIR + STRATOSPHERE_PYTHON_PATH_SUFFIX),
				new Path(STRATOSPHERE_PYTHON_HDFS_PATH), false);
		FileCache.copy(
				new Path(STRATOSPHERE_DIR + STRATOSPHERE_GOOGLE_PATH_SUFFIX),
				new Path(STRATOSPHERE_GOOGLE_HDFS_PATH), false);
		FileCache.copy(
				new Path(STRATOSPHERE_DIR + STRATOSPHERE_EXECUTOR_PATH_SUFFIX),
				new Path(STRATOSPHERE_EXECUTOR_HDFS_PATH), false);
		//copy user package to hdfs
		String distributedPackagePath = STRATOSPHERE_HDFS_PATH + packagePath.substring(packagePath.lastIndexOf("/"));
		FileCache.copy(new Path(tmpPackagePath), new Path(distributedPackagePath), false);
		//copy user package to hdfs
		String distributedPlanPath = STRATOSPHERE_HDFS_PATH + planPath.substring(planPath.lastIndexOf("/"));
		FileCache.copy(new Path(tmpPlanPath), new Path(distributedPlanPath), false);

		//register packages in distributed cache
		env.registerCachedFile(STRATOSPHERE_PYTHON_HDFS_PATH, STRATOSPHERE_PYTHON_ID);
		env.registerCachedFile(STRATOSPHERE_GOOGLE_HDFS_PATH, STRATOSPHERE_GOOGLE_ID);
		env.registerCachedFile(STRATOSPHERE_EXECUTOR_HDFS_PATH, STRATOSPHERE_EXECUTOR_ID);
		env.registerCachedFile(distributedPackagePath, STRATOSPHERE_USER_ID);
		env.registerCachedFile(distributedPlanPath, STRATOSPHERE_PLAN_ID);
	}

	private static void ensureConditions(String planPath, String packagePath) throws IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(new URI(STRATOSPHERE_PYTHON_HDFS_PATH));

		Path[] paths = new Path[]{
			new Path(STRATOSPHERE_PYTHON_HDFS_PATH),
			new Path(STRATOSPHERE_GOOGLE_HDFS_PATH),
			new Path(STRATOSPHERE_EXECUTOR_HDFS_PATH),
			new Path(STRATOSPHERE_HDFS_PATH + packagePath.substring(packagePath.lastIndexOf("/"))),
			new Path(STRATOSPHERE_HDFS_PATH + planPath.substring(planPath.lastIndexOf("/")))
		};

		for (Path path : paths) {
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
		}
	}

	private static void close() throws IOException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(new URI(STRATOSPHERE_PYTHON_HDFS_PATH));
		hdfs.delete(new Path(STRATOSPHERE_HDFS_PATH), true);

		//delete temporary files
		if (!tmpPackagePath.endsWith("stratosphere")) {
			FileSystem local = FileSystem.get(new URI(tmpPackagePath));
			local.delete(new Path(tmpPackagePath), true);
		}
		//delete temporary files
		FileSystem local = FileSystem.get(new URI(tmpPlanPath));
		local.delete(new Path(tmpPlanPath), true);

		sender.close();
		receiver.close();
		process.destroy();
	}

	//====Plan==========================================================================================================
	/**
	 General procedure:
	 Retrieve all operations that are executed on the environment, except input format operations. (for e.g DOP)
	 Retrieve all data sources.
	 For each data source:
	 >>recursion>>
	 receive all sinks
	 receive all operations
	 for every result due to operations:
	 GOTO recursion
	 */
	private static void receivePlan() throws IOException {
		receiveParameters();
		receiveSources();
		receiveSets();
		receiveSinks();
		receiveBroadcast();
	}

	//====Environment===================================================================================================
	/**
	 This enum contains the identifiers for all supported environment parameters.
	 */
	private enum Parameters {
		DOP
	}

	private static void receiveParameters() throws IOException {
		Tuple value;
		value = (Tuple) receiver.receiveSpecialRecord();
		while (value != null) {
			switch (Parameters.valueOf(((String) value.getField(0)).toUpperCase())) {
				case DOP:
					env.setDegreeOfParallelism((Integer) value.getField(1));
			}
			value = (Tuple) receiver.receiveSpecialRecord();
		}
	}

	//====Sources=======================================================================================================
	/**
	 This enum contains the identifiers for all supported InputFormats.
	 */
	private enum InputFormats {
		CSV,
		JDBC,
		TEXT,
		VALUE
	}

	private static void receiveSources() throws IOException {
		Object value;
		while ((value = receiver.receiveSpecialRecord()) != null) {
			String identifier = (String) value;
			switch (InputFormats.valueOf(identifier.toUpperCase())) {
				case CSV:
					createCsvSource();
					break;
				case JDBC:
					createJDBCSource();
					break;
				case TEXT:
					createTextSource();
					break;
				case VALUE:
					createValueSource();
					break;
			}
		}
	}

	private static void createCsvSource() throws IOException {
		int id = (Integer) receiver.receiveSpecialRecord();
		Tuple args = (Tuple) receiver.receiveSpecialRecord();
		Tuple t = createTuple(args.getArity() - 1);
		Class[] classes = new Class[t.getArity()];
		for (int x = 0; x < args.getArity() - 1; x++) {
			t.setField(args.getField(x + 1), x);
			classes[x] = t.getField(x).getClass();
		}
		DataSet<Tuple> set = env.createInput(
				new CsvInputFormat(new Path((String) args.getField(0)), classes),
				getForObject(t));
		sets.put(id, set);
	}

	private static void createJDBCSource() throws IOException {
		int id = (Integer) receiver.receiveSpecialRecord();
		Tuple args = (Tuple) receiver.receiveSpecialRecord();

		DataSet<Tuple> set;
		if (args.getArity() == 3) {
			set = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
					.setDrivername((String) args.getField(0))
					.setDBUrl((String) args.getField(1))
					.setQuery((String) args.getField(2))
					.finish());
			sets.put(id, set);
		}
		if (args.getArity() == 5) {
			set = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
					.setDrivername((String) args.getField(0))
					.setDBUrl((String) args.getField(1))
					.setQuery((String) args.getField(2))
					.setUsername((String) args.getField(3))
					.setPassword((String) args.getField(4))
					.finish());
			sets.put(id, set);
		}
	}

	private static void createTextSource() throws IOException {
		int id = (Integer) receiver.receiveSpecialRecord();
		Tuple args = (Tuple) receiver.receiveSpecialRecord();
		Path path = new Path((String) args.getField(0));
		DataSet<String> set = env.createInput(new TextInputFormat(path), STRING_TYPE_INFO);
		sets.put(id, set);
	}

	private static void createValueSource() throws IOException {
		int id = (Integer) receiver.receiveSpecialRecord();
		int valueCount = (Integer) receiver.receiveSpecialRecord();
		Object[] values = new Object[valueCount];
		for (int x = 0; x < valueCount; x++) {
			values[x] = receiver.receiveSpecialRecord();
		}
		sets.put(id, env.fromElements(values));
	}

	//====Sinks=========================================================================================================
	/**
	 This enum contains the identifiers for all supported OutputFormats.
	 */
	private enum OutputFormats {
		CSV,
		JDBC,
		PRINT,
		TEXT
	}

	private static void receiveSinks() throws IOException {
		Object value;
		while ((value = receiver.receiveSpecialRecord()) != null) {
			int parentID = (Integer) value;
			String identifier = (String) receiver.receiveSpecialRecord();
			Tuple args = (Tuple) receiver.receiveSpecialRecord();
			switch (OutputFormats.valueOf(identifier.toUpperCase())) {
				case CSV:
					createCsvSink(parentID, args);
					break;
				case JDBC:
					createJDBCSink(parentID, args);
					break;
				case PRINT:
					createPrintSink(parentID);
					break;
				case TEXT:
					createTextSink(parentID, args);
					break;
			}
		}
	}

	private static void createCsvSink(int id, Tuple args) {
		((DataSet) sets.get(id)).output(new CsvOutputFormat(new Path((String) args.getField(0))));
	}

	private static void createJDBCSink(int id, Tuple args) {
		switch (args.getArity()) {
			case 3:
				((DataSet) sets.get(id)).output(JDBCOutputFormat.buildJDBCOutputFormat()
						.setDrivername((String) args.getField(0))
						.setDBUrl((String) args.getField(1))
						.setQuery((String) args.getField(2))
						.finish());
				break;
			case 4:
				((DataSet) sets.get(id)).output(JDBCOutputFormat.buildJDBCOutputFormat()
						.setDrivername((String) args.getField(0))
						.setDBUrl((String) args.getField(1))
						.setQuery((String) args.getField(2))
						.setBatchInterval((Integer) args.getField(3))
						.finish());
				break;
			case 5:
				((DataSet) sets.get(id)).output(JDBCOutputFormat.buildJDBCOutputFormat()
						.setDrivername((String) args.getField(0))
						.setDBUrl((String) args.getField(1))
						.setQuery((String) args.getField(2))
						.setUsername((String) args.getField(3))
						.setPassword((String) args.getField(4))
						.finish());
				break;
			case 6:
				((DataSet) sets.get(id)).output(JDBCOutputFormat.buildJDBCOutputFormat()
						.setDrivername((String) args.getField(0))
						.setDBUrl((String) args.getField(1))
						.setQuery((String) args.getField(2))
						.setUsername((String) args.getField(3))
						.setPassword((String) args.getField(4))
						.setBatchInterval((Integer) args.getField(5))
						.finish());
				break;
		}
	}

	private static void createPrintSink(int id) {
		((DataSet) sets.get(id)).output(new PrintingOutputFormat());
	}

	private static void createTextSink(int id, Tuple args) {
		((DataSet) sets.get(id)).output(new TextOutputFormat(new Path((String) args.getField(0))));
	}

	//====Operations====================================================================================================
	/**
	 This enum contains the identifiers for all supported DataSet operations.
	 */
	private enum Operations {
		COGROUP,
		CROSS,
		CROSS_H,
		CROSS_T,
		FILTER,
		FLATMAP,
		GROUPBY,
		GROUPREDUCE,
		JOIN,
		JOIN_H,
		JOIN_T,
		MAP,
		PROJECTION,
		REDUCE,
		SORT,
		UNION
		//aggregate
		//iterate
		//withParameters (cast to UdfOperator)
		//withBroadCastSet (cast to UdfOperator)
	}

	/**
	 General purpose container for all information relating to operations.
	 */
	protected static class OperationInfo {
		protected static final int INFO_MODE_FULL_PRJ = -1;
		protected static final int INFO_MODE_FULL = 0;
		protected static final int INFO_MODE_NOKEY = 1;
		protected static final int INFO_MODE_OPTYPE = 2;
		protected static final int INFO_MODE_OP = 3;
		protected static final int INFO_MODE_GRP = 4;
		protected static final int INFO_MODE_SORT = 5;
		protected static final int INFO_MODE_UNION = 6;
		protected static final int INFO_MODE_PROJECT = 7;
		protected static final int INFO_MODE_NOKEY_PRJ = 8;

		protected int parentID;
		protected int otherID;
		protected int childID;

		protected Tuple keys1;
		protected Tuple keys2;

		protected Tuple projectionKeys1;
		protected Tuple projectionKeys2;

		protected Object types;
		protected String operator;
		protected String meta;

		protected int field;
		protected int order;

		protected OperationInfo(int id, int mode) throws IOException {
			parentID = id;
			childID = (Integer) receiver.receiveSpecialRecord();
			switch (mode) {
				case INFO_MODE_FULL_PRJ:
					keys1 = (Tuple) receiver.receiveSpecialRecord();
					keys2 = (Tuple) receiver.receiveSpecialRecord();
					otherID = (Integer) receiver.receiveSpecialRecord();
					types = (Object) receiver.receiveSpecialRecord();
					operator = (String) receiver.receiveSpecialRecord();
					meta = (String) receiver.receiveSpecialRecord();
					projectionKeys1 = (Tuple) receiver.receiveSpecialRecord();
					projectionKeys2 = (Tuple) receiver.receiveSpecialRecord();
					break;
				case INFO_MODE_NOKEY_PRJ:
					otherID = (Integer) receiver.receiveSpecialRecord();
					types = (Object) receiver.receiveSpecialRecord();
					operator = (String) receiver.receiveSpecialRecord();
					meta = (String) receiver.receiveSpecialRecord();
					projectionKeys1 = (Tuple) receiver.receiveSpecialRecord();
					projectionKeys2 = (Tuple) receiver.receiveSpecialRecord();
					break;
				case INFO_MODE_FULL:
					keys1 = (Tuple) receiver.receiveSpecialRecord();
					keys2 = (Tuple) receiver.receiveSpecialRecord();
					otherID = (Integer) receiver.receiveSpecialRecord();
					types = (Object) receiver.receiveSpecialRecord();
					operator = (String) receiver.receiveSpecialRecord();
					meta = (String) receiver.receiveSpecialRecord();
					break;
				case INFO_MODE_NOKEY:
					otherID = (Integer) receiver.receiveSpecialRecord();
					types = (Object) receiver.receiveSpecialRecord();
					operator = (String) receiver.receiveSpecialRecord();
					meta = (String) receiver.receiveSpecialRecord();
					break;
				case INFO_MODE_OPTYPE:
					types = (Object) receiver.receiveSpecialRecord();
					operator = (String) receiver.receiveSpecialRecord();
					meta = (String) receiver.receiveSpecialRecord();
					break;
				case INFO_MODE_OP:
					operator = (String) receiver.receiveSpecialRecord();
					meta = (String) receiver.receiveSpecialRecord();
					break;
				case INFO_MODE_GRP:
					keys1 = (Tuple) receiver.receiveSpecialRecord();
					break;
				case INFO_MODE_SORT:
					field = (Integer) receiver.receiveSpecialRecord();
					order = (Integer) receiver.receiveSpecialRecord();
					break;
				case INFO_MODE_UNION:
					otherID = (Integer) receiver.receiveSpecialRecord();
					break;
				case INFO_MODE_PROJECT:
					keys1 = (Tuple) receiver.receiveSpecialRecord();
					types = (Object) receiver.receiveSpecialRecord();
					break;
			}
		}
	}

	private static void receiveSets() throws IOException {
		Object value;
		while ((value = receiver.receiveSpecialRecord()) != null) {
			String identifier = (String) value;
			int id = (Integer) receiver.receiveSpecialRecord();
			switch (Operations.valueOf(identifier.toUpperCase())) {
				case COGROUP:
					createCoGroupOperation(new OperationInfo(id, INFO_MODE_FULL));
					break;
				case CROSS:
					createCrossOperation(0, new OperationInfo(id, INFO_MODE_NOKEY_PRJ));
					break;
				case CROSS_H:
					createCrossOperation(1, new OperationInfo(id, INFO_MODE_NOKEY_PRJ));
					break;
				case CROSS_T:
					createCrossOperation(2, new OperationInfo(id, INFO_MODE_NOKEY_PRJ));
					break;
				case FILTER:
					createFilterOperation(new OperationInfo(id, INFO_MODE_OP));
					break;
				case FLATMAP:
					createFlatMapOperation(new OperationInfo(id, INFO_MODE_OPTYPE));
					break;
				case GROUPREDUCE:
					createGroupReduceOperation(new OperationInfo(id, INFO_MODE_OPTYPE));
					break;
				case JOIN:
					createJoinOperation(0, new OperationInfo(id, INFO_MODE_FULL_PRJ));
					break;
				case JOIN_H:
					createJoinOperation(1, new OperationInfo(id, INFO_MODE_FULL_PRJ));
					break;
				case JOIN_T:
					createJoinOperation(2, new OperationInfo(id, INFO_MODE_FULL_PRJ));
					break;
				case MAP:
					createMapOperation(new OperationInfo(id, INFO_MODE_OPTYPE));
					break;
				case PROJECTION:
					createProjectOperation(new OperationInfo(id, INFO_MODE_PROJECT));
					break;
				case REDUCE:
					createReduceOperation(new OperationInfo(id, INFO_MODE_OP));
					break;
				case GROUPBY:
					createGroupOperation(new OperationInfo(id, INFO_MODE_GRP));
					break;
				case SORT:
					createSortOperation(new OperationInfo(id, INFO_MODE_SORT));
					break;
				case UNION:
					createUnionOperation(new OperationInfo(id, INFO_MODE_UNION));
					break;
			}
		}
	}

	private static void createCoGroupOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		int keyCount = info.keys1.getArity();
		int firstKey = info.keys1.getField(0);
		int secondKey = info.keys2.getField(0);
		int[] firstKeys = new int[keyCount - 1];
		int[] secondKeys = new int[keyCount - 1];
		for (int x = 0; x < keyCount - 1; x++) {
			firstKeys[x] = (Integer) info.keys1.getField(x + 1);
			secondKeys[x] = (Integer) info.keys2.getField(x + 1);
		}
		sets.put(info.childID, op1.coGroup(op2).where(firstKey, firstKeys).equalTo(secondKey, secondKeys)
				.with(new PythonCoGroup(info.operator, info.types, info.meta)));
	}

	private static void createCrossOperation(int mode, OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		DefaultCross defaultResult = null;
		switch (mode) {
			case 0:
				defaultResult = op1.cross(op2);
				break;
			case 1:
				defaultResult = op1.crossWithHuge(op2);
				break;
			case 2:
				defaultResult = op1.crossWithTiny(op2);
				break;
			default:
				throw new IllegalArgumentException("Invalid Cross mode specified: " + mode);
		}

		if (info.projectionKeys1 == null & info.projectionKeys2 == null) {
			sets.put(info.childID, defaultResult.with(new PythonCross(info.operator, info.types, info.meta)));
		} else {
			CrossProjection projectionResult = null;
			if (info.projectionKeys1 != null) {
				int[] projectionKeys = new int[info.projectionKeys1.getArity()];
				for (int x = 0; x < projectionKeys.length; x++) {
					projectionKeys[x] = (Integer) info.projectionKeys1.getField(x);
				}
				projectionResult = defaultResult.projectFirst(projectionKeys);
			} else {
				projectionResult = defaultResult.projectFirst();
			}

			if (info.projectionKeys2 != null) {
				int[] projectionKeys = new int[info.projectionKeys2.getArity()];
				for (int x = 0; x < projectionKeys.length; x++) {
					projectionKeys[x] = (Integer) info.projectionKeys2.getField(x);
				}
				projectionResult = projectionResult.projectSecond(projectionKeys);
			} else {
				projectionResult = projectionResult.projectSecond();
			}

			ProjectCross pc;
			Tuple types = (Tuple) info.types;
			switch (types.getArity()) {
				case 1:
					pc = projectionResult.types(
							types.getField(0).getClass());
					break;
				case 2:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass());
					break;
				case 3:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass());
					break;
				case 4:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass());
					break;
				case 5:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass());
					break;
				case 6:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass());
					break;
				case 7:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass());
					break;
				case 8:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass());
					break;
				case 9:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass());
					break;
				case 10:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass());
					break;
				case 11:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass());
					break;
				case 12:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass());
					break;
				case 13:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass());
					break;
				case 14:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass());
					break;
				case 15:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass());
					break;
				case 17:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass());
					break;
				case 18:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass());
					break;
				case 19:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass());
					break;
				case 20:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass());
					break;
				case 21:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass());
					break;
				case 22:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass());
					break;
				case 23:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass());
					break;
				case 24:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass());
					break;
				case 25:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass(),
							types.getField(24).getClass());
					break;
				default:
					throw new IllegalArgumentException("Tuple size not supported");
			}
			sets.put(info.childID, pc);
		}

	}

	private static void createFilterOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.childID, op1.filter(new PythonFilter(info.operator, info.meta)));
	}

	private static void createFlatMapOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.childID, op1.flatMap(new PythonFlatMap(info.operator, info.types, info.meta)));
	}

	private static void createGroupOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		int[] fields = new int[info.keys1.getArity()];
		for (int x = 0; x < fields.length; x++) {
			fields[x] = (Integer) info.keys1.getField(x);
		}
		sets.put(info.childID, op1.groupBy(fields));
	}

	private static void createGroupReduceOperation(OperationInfo info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.childID, ((DataSet) op1)
					.reduceGroup(new PythonGroupReduce(info.operator, info.types, info.meta)));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.childID, ((UnsortedGrouping) op1)
					.reduceGroup(new PythonGroupReduce(info.operator, info.types, info.meta)));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.childID, ((SortedGrouping) op1)
					.reduceGroup(new PythonGroupReduce(info.operator, info.types, info.meta)));
		}
	}

	private static void createJoinOperation(int mode, OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		int keyCount = info.keys1.getArity();
		int firstKey = info.keys1.getField(0);
		int secondKey = info.keys2.getField(0);
		int[] firstKeys = new int[keyCount - 1];
		int[] secondKeys = new int[keyCount - 1];
		for (int x = 0; x < keyCount - 1; x++) {
			firstKeys[x] = (Integer) info.keys1.getField(x + 1);
			secondKeys[x] = (Integer) info.keys2.getField(x + 1);
		}

		DefaultJoin defaultResult = null;
		switch (mode) {
			case 0:
				defaultResult = op1.join(op2).where(firstKey, firstKeys).equalTo(secondKey, secondKeys);
				break;
			case 1:
				defaultResult = op1.joinWithHuge(op2).where(firstKey, firstKeys).equalTo(secondKey, secondKeys);
				break;
			case 2:
				defaultResult = op1.joinWithTiny(op2).where(firstKey, firstKeys).equalTo(secondKey, secondKeys);
				break;
			default:
				throw new IllegalArgumentException("Invalid join mode specified.");
		}

		if (info.projectionKeys1 == null & info.projectionKeys2 == null) {
			sets.put(info.childID, defaultResult.with(new PythonJoin(info.operator, info.types, info.meta)));
		} else {
			JoinProjection projectionResult = null;
			if (info.projectionKeys1 != null) {
				int[] projectionKeys = new int[info.projectionKeys1.getArity()];
				for (int x = 0; x < projectionKeys.length; x++) {
					projectionKeys[x] = (Integer) info.projectionKeys1.getField(x);
				}
				projectionResult = defaultResult.projectFirst(projectionKeys);
			} else {
				projectionResult = defaultResult.projectFirst();
			}

			if (info.projectionKeys2 != null) {
				int[] projectionKeys = new int[info.projectionKeys2.getArity()];
				for (int x = 0; x < projectionKeys.length; x++) {
					projectionKeys[x] = (Integer) info.projectionKeys2.getField(x);
				}
				projectionResult = projectionResult.projectSecond(projectionKeys);
			} else {
				projectionResult = projectionResult.projectSecond();
			}

			ProjectJoin pj;
			Tuple types = (Tuple) info.types;
			switch (types.getArity()) {
				case 1:
					pj = projectionResult.types(
							types.getField(0).getClass());
					break;
				case 2:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass());
					break;
				case 3:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass());
					break;
				case 4:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass());
					break;
				case 5:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass());
					break;
				case 6:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass());
					break;
				case 7:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass());
					break;
				case 8:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass());
					break;
				case 9:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass());
					break;
				case 10:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass());
					break;
				case 11:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass());
					break;
				case 12:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass());
					break;
				case 13:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass());
					break;
				case 14:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass());
					break;
				case 15:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass());
					break;
				case 17:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass());
					break;
				case 18:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass());
					break;
				case 19:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass());
					break;
				case 20:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass());
					break;
				case 21:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass());
					break;
				case 22:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass());
					break;
				case 23:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass());
					break;
				case 24:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass());
					break;
				case 25:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass(),
							types.getField(24).getClass());
					break;
				default:
					throw new IllegalArgumentException("Tuple size not supported");
			}
			sets.put(info.childID, pj);
		}
	}

	private static void createMapOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.childID, op1.map(new PythonMap(info.operator, info.types, info.meta)));
	}

	private static void createProjectOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		Tuple types = (Tuple) info.types;
		int[] fields = new int[info.keys1.getArity()];
		for (int x = 0; x < fields.length; x++) {
			fields[x] = (Integer) info.keys1.getField(x);
		}
		Projection p = op1.project(fields);
		ProjectOperator po;
		switch (types.getArity()) {
			case 1:
				po = p.types(types.getField(0).getClass());
				break;
			case 2:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass());
				break;
			case 3:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass());
				break;
			case 4:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass());
				break;
			case 5:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass());
				break;
			case 6:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass());
				break;
			case 7:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass());
				break;
			case 8:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass());
				break;
			case 9:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass());
				break;
			case 10:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass());
				break;
			case 11:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass());
				break;
			case 12:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass());
				break;
			case 13:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass());
				break;
			case 14:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass());
				break;
			case 15:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass());
				break;
			case 17:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass());
				break;
			case 18:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass());
				break;
			case 19:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass());
				break;
			case 20:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass());
				break;
			case 21:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass());
				break;
			case 22:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
						types.getField(21).getClass());
				break;
			case 23:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
						types.getField(21).getClass(), types.getField(22).getClass());
				break;
			case 24:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
						types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass());
				break;
			case 25:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
						types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass(),
						types.getField(24).getClass());
				break;
			default:
				throw new IllegalArgumentException("Tuple size not supported");
		}
		sets.put(info.childID, po);
	}

	private static void createReduceOperation(OperationInfo info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.childID, ((DataSet) op1).reduce(new PythonReduce(info.operator, info.meta)));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.childID, ((UnsortedGrouping) op1).reduce(new PythonReduce(info.operator, info.meta)));
		}
	}

	private static void createSortOperation(OperationInfo info) {
		Grouping op1 = (Grouping) sets.get(info.parentID);
		Order o;
		switch (info.order) {
			case 0:
				o = Order.NONE;
				break;
			case 1:
				o = Order.ASCENDING;
				break;
			case 2:
				o = Order.DESCENDING;
				break;
			case 3:
				o = Order.ANY;
				break;
			default:
				o = Order.NONE;
				break;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.childID, ((UnsortedGrouping) op1).sortGroup(info.field, o));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.childID, ((SortedGrouping) op1).sortGroup(info.field, o));
		}
	}

	private static void createUnionOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.childID, op1.union(op2));
	}

	//====BroadCastVariables============================================================================================
	private static void receiveBroadcast() throws IOException {
		Object value;
		while ((value = receiver.receiveSpecialRecord()) != null) {
			int parentID = (Integer) value;
			int otherID = (Integer) receiver.receiveSpecialRecord();
			String name = (String) receiver.receiveSpecialRecord();
			DataSet op1 = (DataSet) sets.get(parentID);
			DataSet op2 = (DataSet) sets.get(otherID);

			if (op1 instanceof SingleInputUdfOperator) {
				((SingleInputUdfOperator) op1).withBroadcastSet(op2, name);
				return;
			}
			if (op1 instanceof TwoInputUdfOperator) {
				((TwoInputUdfOperator) op1).withBroadcastSet(op2, name);
				return;
			}
		}
	}
}