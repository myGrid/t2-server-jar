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

import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.AbstractList;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrBuilder;

/**
 * 
 * @author Robert Haines
 */
public abstract class PortValue extends AbstractList<PortValue> {

	public static final String PORT_ERROR_TYPE = "application/x-error";
	public static final String PORT_LIST_TYPE = "application/x-list";

	private final Run run;
	private final URI reference;
	private final String type;
	protected final long size;

	PortValue(Port parent, URI reference, String type, long size) {
		this.run = parent.getRun();
		this.reference = reference;
		this.type = type;
		this.size = size;
	}

	public String getContentType() {
		return type;
	}

	public abstract byte[] getData();

	public abstract byte[] getData(int index);

	public abstract InputStream getDataStream();

	public String getDataAsString() {
		return new String(getData());
	}

	public abstract long getDataSize();

	public URI getReference() {
		return reference;
	}

	public Run getRun() {
		return run;
	}

	public abstract boolean isError();

	@Override
	public String toString() {
		return toString(0);
	}

	public String toString(int indent) {
		String spaces = StringUtils.repeat(" ", indent);
		StrBuilder message = new StrBuilder();
		PrintWriter pw = new PrintWriter(message.asWriter());

		pw.format("%sReference:    %s\n", spaces, reference.toASCIIString());
		pw.format("%sContent type: %s\n", spaces, type);
		pw.format("%sData size:    %d", spaces, getDataSize());

		return message.toString();
	}
}
