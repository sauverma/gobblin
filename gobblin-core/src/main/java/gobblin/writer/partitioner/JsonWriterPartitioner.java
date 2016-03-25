package gobblin.writer.partitioner;

import java.nio.charset.StandardCharsets;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.State;

/**
 * 
 * @author sauverma
 *
 *	Mark timestamp field in the input record
 *	Only ISO8601 format for TS is supported : YYYY-MM-ddTHH:mm:SS.sssZ
 *
 *	Metrics to be emitted for messages where exception is thrown while parsing
 */

public class JsonWriterPartitioner extends TimeBasedWriterPartitioner<byte[]> {
	public final static String TIMESTAMP_COLUMN = "kafka.json.record.timestampcolumn";
	public final static String DEFAULT_TIMESTAMP_COLUMN = "timestampInMs";
	public String tsColumnName = null;
	private Gson gson = new Gson();

	public JsonWriterPartitioner(State state, int numBranches, int branchId)  {
		super(state, numBranches, branchId);
		tsColumnName = state.getProp(TIMESTAMP_COLUMN, DEFAULT_TIMESTAMP_COLUMN);
	}

	@Override
	public long getRecordTimestamp(byte[] record) {
		String message = new String(record, StandardCharsets.UTF_8);
		JsonObject jsonObject = null;

		try {
			jsonObject = gson.fromJson(message, JsonObject.class);
		} catch (RuntimeException e) {
			System.out.println(e);
			//throw new RuntimeException(e.getCause() + " : " + message);
		}

		long ret = System.currentTimeMillis();
		if (jsonObject != null && jsonObject.has(tsColumnName)) {
			JsonElement ts = jsonObject.get(tsColumnName); 
			DateTime dTs = new DateTime(ts.getAsString(), DateTimeZone.UTC);
			ret = dTs.getMillis();
		}

		return ret;
	}
}
