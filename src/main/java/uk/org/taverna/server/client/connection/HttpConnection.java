/*
 * Copyright (c) 2010-2012 The University of Manchester, UK.
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

package uk.org.taverna.server.client.connection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;

import org.apache.commons.lang.math.IntRange;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import uk.org.taverna.server.client.AccessForbiddenException;
import uk.org.taverna.server.client.AttributeNotFoundException;
import uk.org.taverna.server.client.AuthorizationException;
import uk.org.taverna.server.client.InternalServerException;
import uk.org.taverna.server.client.UnexpectedResponseException;
import uk.org.taverna.server.client.connection.params.ConnectionParams;

/**
 * 
 * @author Robert Haines
 */
public class HttpConnection implements Connection {

	protected final URI uri;

	protected final ConnectionParams params;

	protected final HttpClient httpClient;
	protected final HttpContext httpContext;

	HttpConnection(URI uri, ConnectionParams params) {
		this.uri = uri;
		this.params = params;
		httpClient = new DefaultHttpClient(new BasicHttpParams());
		httpContext = new BasicHttpContext();
	}

	@Override
	public String upload(URI uri, byte[] content, String type,
			UserCredentials credentials) {
		return upload(uri, new ByteArrayInputStream(content), content.length,
				type, credentials);
	}

	@Override
	public String upload(URI uri, InputStream content, String type,
			UserCredentials credentials) {
		return upload(uri, content, -1, type, credentials);
	}

	private String upload(URI uri, InputStream content, long length,
			String type, UserCredentials credentials) {
		HttpPost request = new HttpPost(uri);
		String location = null;

		if (credentials != null) {
			credentials.authenticate(request, httpContext);
		}

		try {
			InputStreamEntity entity = new InputStreamEntity(content, length);
			entity.setContentType(type);
			request.setEntity(entity);

			HttpResponse response = httpClient.execute(request, httpContext);

			processResponse(response, HttpURLConnection.HTTP_CREATED, uri);
			location = response.getHeaders("location")[0].getValue();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return location;
	}

	@Override
	public byte[] getAttribute(URI uri, UserCredentials credentials) {
		return getAttribute(uri, null, null, credentials);
	}

	@Override
	public byte[] getAttribute(URI uri, String type,
			UserCredentials credentials) {
		return getAttribute(uri, type, null, credentials);
	}

	@Override
	public byte[] getAttribute(URI uri, String type, IntRange range,
			UserCredentials credentials) {
		HttpGet request = new HttpGet(uri);
		int success = HttpURLConnection.HTTP_OK;

		if (type != null) {
			request.addHeader("Accept", type);
		}

		if (range != null) {
			request.addHeader("Range", "bytes=" + range.getMinimumInteger()
					+ "-" + range.getMaximumInteger());
			success = HttpURLConnection.HTTP_PARTIAL;
		}

		if (credentials != null) {
			credentials.authenticate(request, httpContext);
		}

		HttpResponse response;
		try {
			response = httpClient.execute(request, httpContext);

			return processResponse(response, success, uri);
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void setAttribute(URI uri, String value, String type,
			UserCredentials credentials) {
		HttpPut request = new HttpPut(uri);

		if (credentials != null) {
			credentials.authenticate(request, httpContext);
		}

		try {
			StringEntity content = new StringEntity(value, "UTF-8");
			content.setContentType(type);
			request.setEntity(content);

			HttpResponse response = httpClient.execute(request, httpContext);
			processResponse(response, HttpURLConnection.HTTP_OK, uri);
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void delete(URI uri, UserCredentials credentials) {
		HttpDelete request = new HttpDelete(uri);

		if (credentials != null) {
			credentials.authenticate(request, httpContext);
		}

		try {
			HttpResponse response = httpClient.execute(request, httpContext);
			processResponse(response, HttpURLConnection.HTTP_NO_CONTENT, uri);
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private byte[] processResponse(HttpResponse response, int success,
			URI requestURI) throws IOException {
		int status = response.getStatusLine().getStatusCode();

		// get the entity from the response.
		HttpEntity entity = response.getEntity();

		// if the response is successful, return the pay-load.
		if (status == success) {
			if (entity == null) {
				return null;
			}
			return EntityUtils.toByteArray(entity);
		}

		// if we get here we need to consume the entity. This has the side
		// effect of resetting the HTTP connection so it MUST be done every
		// time. We need to save any content first for error messages.
		String content = null;
		if (entity != null) {
			content = EntityUtils.toString(entity);
			EntityUtils.consume(entity);
		}

		switch (status) {
		case HttpURLConnection.HTTP_NOT_FOUND:
			throw new AttributeNotFoundException(requestURI);
		case HttpURLConnection.HTTP_FORBIDDEN:
			throw new AccessForbiddenException(requestURI);
		case HttpURLConnection.HTTP_UNAUTHORIZED:
			throw new AuthorizationException();
		case HttpURLConnection.HTTP_INTERNAL_ERROR:
			String message = (content != null) ? content : "<not specified>";
			throw new InternalServerException(message);
		default:
			String error = status + " ("
					+ response.getStatusLine().getReasonPhrase()
					+ ") while accessing '" + requestURI + "'";
			error += (content != null) ? " - " + content : " - <not specified>";

			throw new UnexpectedResponseException(error);
		}
	}
}
