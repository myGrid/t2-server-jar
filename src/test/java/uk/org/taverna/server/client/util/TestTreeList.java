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

package uk.org.taverna.server.client.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class TestTreeList {

	private TreeList<Integer> emptyb;
	private TreeList<Integer> emptyc;
	private TreeList<Integer> none;
	private TreeList<Integer> il10;
	private TreeList<Integer> il20;
	private TreeList<Integer> ic30;
	private TreeList<Integer> ib;
	private TreeList<Integer> ic;

	@Before
	public void setup() {
		emptyb = new TreeList<Integer>();
		emptyc = new TreeList<Integer>(true);
		none = null;

		il10 = new TreeList<Integer>(10);
		il20 = new TreeList<Integer>(20);

		ib = new TreeList<Integer>();
		ib.addNode(il10);
		ib.addNode(il20);

		ic30 = new TreeList<Integer>(true, 30);
		ic30.addNode(il20);

		ic = new TreeList<Integer>(true, 10);
		ic.addNode(il10);
		ic.addNode(ib);
		ic.addNode(ic30);
	}

	@Test
	public void testIsLeaf() {
		assertTrue(il10.isLeaf());
		assertFalse(il10.isComposite());
	}

	@Test
	public void testIsBranch() {
		assertTrue(ib.isBranch());
		assertFalse(ib.isComposite());
	}

	@Test
	public void testIsComposite() {
		assertTrue(ic.isLeaf());
		assertTrue(ic.isBranch());
		assertTrue(ic.isComposite());
	}

	@Test(expected = TreeListTypeMismatchException.class)
	public void testGetValue() {
		assertEquals((Integer) 10, il10.getValue());
		ib.getValue();
	}

	@Test(expected = TreeListTypeMismatchException.class)
	public void testGetValueDeep() {
		assertEquals((Integer) 10, ib.getValue(0));
		assertEquals((Integer) 20, ic.getValue(1, 1));
		il10.getValue(0);
	}

	@Test(expected = TreeListTypeMismatchException.class)
	public void testSetValue() {
		il10.setValue(20);
		assertEquals((Integer) 20, il10.getValue());
		ib.setValue(0);
	}

	@Test(expected = TreeListTypeMismatchException.class)
	public void testGetNode() {
		assertSame(il10, ib.getNode(0));
		assertEquals((Integer) 20, ib.getNode(1).getValue());
		il10.getNode(0);
	}

	@Test(expected = TreeListTypeMismatchException.class)
	public void testSetNode() {
		TreeList<Integer> n = ib.setNode(0, il20);
		assertSame(il10, n);
		assertSame(il20, ib.getNode(0));
		il10.setNode(0, il20);
	}

	@Test(expected = TreeListTypeMismatchException.class)
	public void testAddNodeTreeListOfE() {
		ib.addNode(il20);
		assertSame(il20, ib.getNode(2));

		ib.addNode(none);
		assertNull(ib.getNode(3));

		il10.addNode(il20);
	}

	@Test(expected = TreeListTypeMismatchException.class)
	public void testAddNodeIntTreeListOfE() {
		ib.addNode(0, il20);
		assertSame(il20, ib.getNode(0));
		assertSame(il10, ib.getNode(1));

		il10.addNode(0, il20);
	}

	@Test(expected = TreeListTypeMismatchException.class)
	public void testRemoveNodeInt() {
		TreeList<Integer> n = ib.removeNode(0);
		assertSame(il10, n);
		assertSame(il20, ib.getNode(0));

		il10.removeNode(0);
	}

	@Test(expected = TreeListTypeMismatchException.class)
	public void testRemoveNodeObject() {
		assertTrue(ib.removeNode(il10));

		il10.removeNode(il20);
	}

	@Test(expected = TreeListTypeMismatchException.class)
	public void testContainsNode() {
		assertTrue(ib.containsNode(il20));
		assertFalse(ib.containsNode(ic30));

		il10.containsNode(il10);
	}

	@Test
	public void testClear() {
		il10.clear();
		assertNull(il10.getValue());

		ib.clear();
		assertEquals(0, ib.size());
	}

	@Test
	public void testSize() {
		assertEquals(0, il10.size());
		assertEquals(0, emptyb.size());
		assertEquals(1, ic30.size());
		assertEquals(2, ib.size());
		assertEquals(3, ic.size());
	}

	@Test
	public void testToString() {
		assertEquals("null: [null]", emptyc.toString());
		assertEquals("20", il20.toString());
		assertEquals("[10, 20]", ib.toString());
		assertEquals("10: [10, [10, 20], 30: [20]]", ic.toString());
	}
}
