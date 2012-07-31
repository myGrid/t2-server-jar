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

import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrBuilder;

import uk.org.taverna.server.client.xml.port.ErrorValue;
import uk.org.taverna.server.client.xml.port.LeafValue;
import uk.org.taverna.server.client.xml.port.ListValue;
import uk.org.taverna.server.client.xml.port.Value;

/**
 * 
 * @author Robert Haines
 */
public final class OutputPort extends Port {

	private final PortValue value;

	OutputPort(Run run, uk.org.taverna.server.client.xml.port.OutputPort port) {
		super(run, port.getName(), port.getDepth());

		LeafValue v = port.getValue();
		ListValue lv = port.getList();
		ErrorValue ev = port.getError();

		PortValue value = null;
		if (v != null) {
			value = new PortData(this, v.getHref(), v.getContentType(),
					v.getContentByteLength());
		} else if (lv != null) {
			value = parse(lv);
		} else if (ev != null) {
			value = new PortError(this, ev.getHref());
		}

		this.value = value;
	}

	/**
	 * 
	 * @return
	 */
	public PortValue getValue() {
		return value;
	}

	/**
	 * Does this port contain error?
	 * 
	 * @return <code>true</code> if this port contains an error,
	 *         <code>false</code> otherwise.
	 */
	public boolean isError() {
		return value.isError();
	}

	/**
	 * Get the total size of all the data on this OutputPort. For a singleton
	 * port this is simply the size of the single value but for any other depth
	 * port it is the addition of all values in the port.
	 * 
	 * @return The total data size of this OutputPort.
	 */
	public long getDataSize() {
		return value.getDataSize();
	}

	public String getContentType() {
		return value.getContentType();
	}

	public byte[] getData() {
		return value.getData();
	}

	public byte[] getData(int index) {
		return value.getData(index);
	}

	public String getDataAsString() {
		return value.getDataAsString();
	}

	public URI getReference() {
		return value.getReference();
	}

	@Override
	public String toString() {
		return toString(0);
	}

	public String toString(int indent) {
		String spaces = StringUtils.repeat(" ", indent);
		StrBuilder message = new StrBuilder();
		PrintWriter pw = new PrintWriter(message.asWriter());

		pw.format("%s%s", spaces, getName());
		pw.format("%s (depth %s) {\n", spaces, getDepth());
		pw.format("%s%s\n}", spaces, value.toString(indent + 1));

		return message.toString();
	}

	/*
	 * This method has to parse the OutputPort structure by trying to cast to
	 * each type that a port can be. Not pretty.
	 * 
	 * Even though we know that first time through this method we must have a
	 * list we try to cast to a value first as this is what we will most often
	 * have.
	 */
	private PortValue parse(Value value) {
		try {
			LeafValue lv = (LeafValue) value;

			return new PortData(this, lv.getHref(), lv.getContentType(), lv.getContentByteLength());
		} catch (ClassCastException e) {
			// Ignore this error and try the next cast!
		}

		try {
			ListValue lv = (ListValue) value;

			List<PortValue> list = new ArrayList<PortValue>();
			for (Value v : lv.getValueOrListOrError()) {
				list.add(parse(v));
			}

			return new PortList(this, lv.getHref(), list);
		} catch (ClassCastException e) {
			// Ignore this error and try the next cast!
		}

		try {
			ErrorValue ev = (ErrorValue) value;

			return new PortError(this, ev.getHref());
		} catch (ClassCastException e) {
			// Hmmm...
		}

		// We should NOT get here!
		return null;
	}
}
