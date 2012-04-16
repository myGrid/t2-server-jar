/*
 * Copyright (c) 2012 The University of Manchester, UK.
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * * Neither the names of The University of Manchester nor the names of its
 *   contributors may be used to endorse or promote products derived from this
 *   software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package uk.org.taverna.server.client;

import java.net.URI;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.math.IntRange;

public final class PortValue {

	private final Run run;
	private final URI reference;
	private final String type;
	private final int size;
	private String error;
	private byte[] data;
	private IntRange dataGot;

	// If there is no data...
	private static final byte[] EMPTY_DATA = new byte[0];

	private PortValue(OutputPort port, URI reference, boolean error,
			String type, int size) {
		this.run = port.getRun();
		this.reference = reference;
		this.size = size;
		this.dataGot = null;
		this.error = null;

		if (error) {
			this.type = "application/x-error";
		} else {
			this.type = type;
		}

		if (this.type.equalsIgnoreCase("application/x-empty")) {
			this.data = EMPTY_DATA;
		} else {
			this.data = null;
		}
	}

	/**
	 * Create a new PortValue that represents a normal data value.
	 * 
	 * @param port
	 *            The parent OutputPort of this value.
	 * @param reference
	 *            The URI from which to retrieve the data for this value.
	 * @param type
	 * @param size
	 */
	PortValue(OutputPort port, URI reference, String type, int size) {
		this(port, reference, false, type, size);
	}

	/**
	 * Create a new PortValue that represents an error.
	 * 
	 * @param port
	 *            The parent OutputPort of this value.
	 * @param reference
	 *            The URI from which to retrieve the error message.
	 */
	PortValue(OutputPort port, URI reference) {
		this(port, reference, true, "", 0);
	}

	/**
	 * Get the URI reference to the data of this output port value.
	 * 
	 * @return The URI reference to the data of this output port value.
	 */
	public URI getReference() {
		return reference;
	}

	/**
	 * Get the error string of this output port value. If this value is not an
	 * error then <code>null</code> is returned.
	 * 
	 * @return The error message (or <code>null</code>) associated with this
	 *         output port value.
	 */
	public String getError() {
		if (error == null) {
			error = new String(run.getOutputData(this.reference, null));
		}

		return error;
	}

	/**
	 * Does this output port value represent an error?
	 * 
	 * @return <code>true</code> if this output port value is an error
	 *         <code>false</code> otherwise.
	 */
	public boolean isError() {
		return (type.equalsIgnoreCase("application/x-error"));
	}

	/**
	 * Get the mime type of the data at this output port value. If the value is
	 * missing then <code>application/x-empty</code> is returned. If the value
	 * is an error then <code>application/x-error</code> is returned.
	 * 
	 * @return The mime type of this output port value.
	 */
	public String getDataType() {
		return type;
	}

	/**
	 * Get the size of the data of this output port value.
	 * 
	 * @return The data size of this output port value.
	 */
	public int getDataSize() {
		return size;
	}

	/**
	 * Get all the data from this output port value as a String.
	 * 
	 * @return The data from this output port.
	 */
	public String getStringData() {
		return new String(getData());
	}

	/**
	 * Get all the data from this output port value.
	 * 
	 * @return The data from this output port.
	 */
	public byte[] getData() {
		// IntRange is inclusive so size is too long by one.
		return getData(new IntRange(0, (size - 1)));
	}

	/**
	 * Get the specified range of data from this output port value.
	 * 
	 * @param start
	 *            The start position in the data to get.
	 * @param length
	 *            The length of data to get.
	 * @return The requested data.
	 */
	public byte[] getData(int start, int length) {
		// If length is zero then there is nothing to return.
		if (length == 0) {
			return EMPTY_DATA;
		}

		// IntRange is inclusive so (start + length) is too long by one.
		return getData(new IntRange(start, (start + length - 1)));
	}

	/**
	 * Get the specified range of data from this output port value.
	 * 
	 * @param from
	 *            The start of the range.
	 * @param to
	 *            The end of the range.
	 * @param isInclusive
	 *            Whether the end of the range is included or not.
	 * @return The requested data.
	 */
	public byte[] getData(int from, int to, boolean isInclusive) {
		// IntRange is inclusive so an exclusive range is too long by one. An
		// exclusive range where from == to should yield nothing.
		if (!isInclusive) {
			if (from == to) {
				return EMPTY_DATA;
			}

			to--;
		}

		return getData(new IntRange(from, to));
	}

	/**
	 * Get the specified range of data from this output port value.
	 * 
	 * @param range
	 *            The byte range of the data to download from the server.
	 * @return The requested data.
	 */
	private byte[] getData(IntRange range) {
		// There is no data for an error.
		if (isError()) {
			return null;
		}

		// Return empty data if this value is empty.
		if (size == 0 || type.equalsIgnoreCase("application/x-empty")) {
			return EMPTY_DATA;
		}

		// Check the range provided is sensible. IntRange is inclusive so size
		// is too long by one.
		if (range.getMinimumInteger() < 0) {
			range = new IntRange(0, range.getMaximumInteger());
		}
		if (range.getMaximumInteger() >= size) {
			range = new IntRange(range.getMinimumInteger(), (size - 1));
		}

		// Find the data range(s) that we need to download.
		IntRange[] need = fill(dataGot, range);

		switch (need.length) {
		case 0:
			// We already have all the data we need, just return the right bit.
			// dataGot cannot be null here and must fully encompass range.
			int from = range.getMinimumInteger() - dataGot.getMinimumInteger();
			int to = range.getMaximumInteger() - dataGot.getMinimumInteger();

			// copyOfRange is exclusive!
			return Arrays.copyOfRange(data, from, (to + 1));
		case 1:
			// we either have some data, at one end of range or either side of
			// it, or none. dataGot can be null here.
			// In both cases we download what we need.
			byte[] newData = run.getOutputData(reference, need[0]);
			if (dataGot == null) {
				// This is the only data we have, return it all.
				dataGot = range;
				data = newData;
				return data;
			} else {
				// Add the new data to the correct end of the data we have,
				// then return the range requested.
				if (range.getMaximumInteger() <= dataGot.getMaximumInteger()) {
					dataGot = new IntRange(range.getMinimumInteger(),
							dataGot.getMaximumInteger());
					data = ArrayUtils.addAll(newData, data);

					// copyOfRange is exclusive!
					return Arrays.copyOfRange(data, 0,
							(range.getMaximumInteger() + 1));
				} else {
					dataGot = new IntRange(dataGot.getMinimumInteger(),
							range.getMaximumInteger());
					data = ArrayUtils.addAll(data, newData);

					// copyOfRange is exclusive!
					return Arrays.copyOfRange(data,
							(range.getMinimumInteger() - dataGot
									.getMinimumInteger()), (dataGot
											.getMaximumInteger() + 1));
				}
			}
		case 2:
			// We definitely have some data and it is in the middle of the
			// range requested. dataGot cannot be null here.
			dataGot = range;
			byte[] data1 = run.getOutputData(reference, need[0]);
			byte[] data2 = run.getOutputData(reference, need[1]);
			data = ArrayUtils.addAll(data1, data);
			data = ArrayUtils.addAll(data, data2);
			return data;
		}

		// Should never get here! This is an error!
		return null;
	}

	@Override
	public String toString() {
		if (isError()) {
			return getError();
		} else {
			return getStringData();
		}
	}

	// Aaaarrrgh!
	private IntRange[] fill(IntRange got, IntRange want) {
		if (got == null) {
			return new IntRange[] { want };
		}

		if (got.containsInteger(want.getMinimumInteger())) {
			if (got.containsInteger(want.getMaximumInteger())) {
				return new IntRange[0];
			} else {
				return new IntRange[] { new IntRange(
						(got.getMaximumInteger() + 1),
						want.getMaximumInteger()) };
			}
		} else {
			if (got.containsInteger(want.getMaximumInteger())) {
				return new IntRange[] { new IntRange(want.getMinimumInteger(),
						(got.getMinimumInteger() - 1)) };
			} else {
				if (want.getMaximumInteger() < got.getMinimumInteger()) {
					return new IntRange[] { new IntRange(
							want.getMinimumInteger(),
							(got.getMinimumInteger() - 1)) };
				} else if (want.getMinimumInteger() > got.getMaximumInteger()) {
					return new IntRange[] { new IntRange(
							(got.getMaximumInteger() + 1),
							want.getMaximumInteger()) };
				} else {
					return new IntRange[] {
							new IntRange(want.getMinimumInteger(),
									(got.getMinimumInteger() - 1)),
									new IntRange((got.getMaximumInteger() + 1),
											want.getMaximumInteger()) };
				}
			}
		}
	}
}
