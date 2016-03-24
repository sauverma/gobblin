/*
 * Copyright (C) 2014-2016 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import gobblin.configuration.WorkUnitState;
import kafka.message.MessageAndOffset;

/**
 * An implementation of {@link KafkaExtractor} from which reads and returns
 * records as an array of bytes.
 *
 * @author saurabh.verma@zeotap.com
 */
public class KafkaSimpleJsonExtractorRecordDelim extends KafkaExtractor<String, byte[]> {

	private static final Map<String, String> delimMap = new ConcurrentHashMap<String, String>();
	private static final String ROWDELIMTER = "kafka.output.record.delimiter";
	private static final String DEFAULT_RECORD_DELIM = "NEWLINE";
	private static String allowedDelimiters = null;
	private String recordDelimiter = null;

	static {
		delimMap.put("NEWLINE", "\n");
		delimMap.put("BLANK", "");

		for (String delim : delimMap.keySet()) {
			allowedDelimiters += "[" + delim + "]\n";
		}
	}

	public KafkaSimpleJsonExtractorRecordDelim(WorkUnitState state) throws IllegalArgumentException {
		super(state);
		String inpDelim = delimMap.get(state.getProp(ROWDELIMTER, DEFAULT_RECORD_DELIM));
		if (inpDelim == null) {
			throw new IllegalArgumentException(
					"Invalid value for kafka.output.record.delimiter, can be one of : " + allowedDelimiters);
		}

		recordDelimiter = inpDelim;
	}

	@Override
	protected byte[] decodeRecord(MessageAndOffset messageAndOffset) throws IOException {

		ByteBuffer delimPayload = ByteBuffer
				.allocate(messageAndOffset.message().payloadSize() + "\n".getBytes().length);
		delimPayload.put(messageAndOffset.message().payload());
		delimPayload.put(recordDelimiter.getBytes());
		delimPayload.flip();

		return getBytes(delimPayload);
	}

	/**
	 * Get the schema (metadata) of the extracted data records.
	 *
	 * @return the Kafka topic being extracted
	 * @throws IOException
	 *             if there is problem getting the schema
	 */
	@Override
	public String getSchema() throws IOException {
		return this.topicName;
	}
}
