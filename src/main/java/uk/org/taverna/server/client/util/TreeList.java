/*
 * Copyright (c) 2010, 2011 The University of Manchester, UK.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 
 * @author Robert Haines
 * 
 * @param <E>
 */
public class TreeList<E> implements Cloneable, Iterable<TreeList<E>> {

	// the children of this node
	protected List<TreeList<E>> branch;

	// the payload of this node
	protected E leaf;

	// can this node be both a leaf and a branch?
	private final boolean composite;

	/**
	 * Create a new branch node with no initial value.
	 */
	public TreeList() {
		this(false);
	}

	/**
	 * Create a new node with no initial value.
	 * 
	 * @param composite
	 */
	public TreeList(boolean composite) {
		this.composite = composite;
		leaf = null;
		branch = new ArrayList<TreeList<E>>();
	}

	/**
	 * Create a new leaf node with an initial value.
	 * 
	 * @param value
	 */
	public TreeList(E value) {
		this(false, value);
	}

	/**
	 * Create a new node with an initial value.
	 * 
	 * @param composite
	 * @param value
	 */
	public TreeList(boolean composite, E value) {
		this.composite = composite;
		leaf = value;
		branch = composite ? new ArrayList<TreeList<E>>() : null;
	}

	/**
	 * 
	 * @param composite
	 * @param list
	 */
	protected TreeList(boolean composite, List<TreeList<E>> list) {
		this.composite = composite;
		leaf = null;
		branch = list != null ? list : new ArrayList<TreeList<E>>();
	}

	/**
	 * 
	 * @param list
	 */
	protected TreeList(List<TreeList<E>> list) {
		this(false, list);
	}

	/**
	 * 
	 * @param value
	 * @param list
	 */
	protected TreeList(E value, List<TreeList<E>> list) {
		composite = true;
		leaf = value;
		branch = list != null ? list : new ArrayList<TreeList<E>>();
	}

	/**
	 * Is this node a leaf? Composite nodes are both leaf and branch nodes.
	 * 
	 * @return true if this node is a leaf, false otherwise.
	 */
	public boolean isLeaf() {
		return composite || branch == null;
	}

	/**
	 * Is this node a branch? Composite nodes are both leaf and branch nodes.
	 * 
	 * @return true if this node is a branch, false otherwise.
	 */
	public boolean isBranch() {
		return composite || branch != null;
	}

	/**
	 * Is this node a composite node?
	 * 
	 * @return true if this is a composite node, false otherwise.
	 */
	public boolean isComposite() {
		return composite;
	}

	/**
	 * 
	 * @return the value stored in this node.
	 * @throws TreeListTypeMismatchException
	 *             if this is not a leaf node.
	 */
	public E getValue() throws TreeListTypeMismatchException {
		if (!isLeaf()) {
			throw new TreeListTypeMismatchException("leaf");
		}

		return leaf;
	}

	/**
	 * 
	 * @param data
	 * @return
	 * @throws TreeListTypeMismatchException
	 *             if this is not a leaf node.
	 */
	public E setValue(E data) throws TreeListTypeMismatchException {
		if (!isLeaf()) {
			throw new TreeListTypeMismatchException("leaf");
		}

		E temp = leaf;
		leaf = data;
		return temp;
	}

	/**
	 * 
	 * @param index
	 * @return
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public TreeList<E> getNode(int index) throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.get(index);
	}

	/**
	 * 
	 * @param index
	 * @param node
	 * @return
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public TreeList<E> setNode(int index, TreeList<E> node)
			throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		TreeList<E> temp = branch.get(index);
		branch.set(index, node);
		return temp;
	}

	/**
	 * 
	 * @param node
	 * @return
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public boolean addNode(TreeList<E> node)
			throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.add(node);
	}

	/**
	 * 
	 * @param index
	 * @param node
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public void addNode(int index, TreeList<E> node)
			throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		branch.add(index, node);
	}

	/**
	 * 
	 * @param index
	 * @return
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public TreeList<E> removeNode(int index)
			throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.remove(index);
	}

	/**
	 * 
	 * @param o
	 * @return
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public boolean removeNode(Object o) throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.remove(o);
	}

	/**
	 * 
	 * @param o
	 * @return
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public boolean containsNode(Object o) throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.contains(o);
	}

	/**
	 * Removes all of the nodes from this node's branch and sets the value of
	 * this node to null.
	 */
	public void clear() {
		leaf = null;

		if (isBranch()) {
			branch.clear();
		}
	}

	/**
	 * Returns the number of nodes on this branch or 0 if it is a leaf nodes.
	 * 
	 * @return the number of nodes on this branch or 0 if it is a leaf nodes.
	 */
	public int size() {
		return isBranch() ? branch.size() : 0;
	}

	@Override
	public String toString() {
		String result = "";

		if (isLeaf()) {
			result = leaf == null ? "null" : leaf.toString();
		}

		if (isComposite()) {
			result += ": ";
		}

		if (isBranch()) {
			if (branch.size() == 0) {
				result += "[null]";
			} else {
				result += "[" + branch.get(0);
				for (int i = 1; i < branch.size(); i++) {
					result += ", " + branch.get(i);
				}
				result += "]";
			}
		}

		return result;
	}

	/**
	 * Returns a shallow copy of this TreeList instance. (The value and node
	 * elements themselves are not copied.)
	 * 
	 * @return a clone of this TreeList instance
	 */
	@Override
	public Object clone() {
		if (this.isComposite()) {
			return new TreeList<E>(this.leaf, this.branch);
		} else {
			if (this.isLeaf()) {
				return new TreeList<E>(this.leaf);
			} else {
				return new TreeList<E>(this.branch);
			}
		}
	}

	@Override
	public Iterator<TreeList<E>> iterator()
			throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.iterator();
	}
}
