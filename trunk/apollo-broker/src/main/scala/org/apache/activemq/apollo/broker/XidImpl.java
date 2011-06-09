/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.broker;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.transaction.xa.Xid;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;

/**
 * An implementation of JTA transaction identifier (javax.transaction.xa.Xid).
 */
public class XidImpl implements Xid, Cloneable, java.io.Serializable {

    private static final long serialVersionUID = -5363901495878210611L;
    private static final Buffer EMPTY_BUFFER = new Buffer(new byte[]{});

    // The format identifier for the XID. A value of -1 indicates the NULLXID
    private int formatId = -1; // default format
    Buffer globalTransactionId = EMPTY_BUFFER;
    Buffer branchQualifier = EMPTY_BUFFER;

    /////////////////////////////// Constructors /////////////////////////////
    /**
     * Constructs a new null XID.
     * <p>
     * After construction the data within the XID should be initialized.
     */
    public XidImpl() {
    }

    public XidImpl(int formatID, byte[] globalTxnID, byte[] branchID) {
        this.formatId = formatID;
        setGlobalTransactionId(globalTxnID);
        setBranchQualifier(branchID);
    }

    public XidImpl(int formatID, Buffer globalTransactionId,  Buffer branchQualifier) {
        this.formatId = formatID;
        this.globalTransactionId = globalTransactionId;
        this.branchQualifier=branchQualifier;
    }

    /**
     * Initialize an XID using another XID as the source of data.
     *
     * @param from
     *            the XID to initialize this XID from
     */
    public XidImpl(Xid from) {
        if ((from == null) || (from.getFormatId() == -1)) {
            formatId = -1;
            setGlobalTransactionId(null);
            setBranchQualifier(null);
        } else {
            formatId = from.getFormatId();
            setGlobalTransactionId(from.getGlobalTransactionId());
            setBranchQualifier(from.getBranchQualifier());
        }

    }

    // used for test purpose
    public XidImpl(String globalTxnId, String branchId) {
        this(99, globalTxnId.getBytes(), branchId.getBytes());
    }

    //////////// Public Methods //////////////

    /**
     * Determine whether or not two objects of this type are equal.
     *
     * @param o
     *            the other XID object to be compared with this XID.
     *
     * @return Returns true of the supplied object represents the same global
     *         transaction as this, otherwise returns false.
     */
    public boolean equals(Object o) {
        if (o.getClass() != XidImpl.class)
            return false;

        XidImpl other = (XidImpl) o;
        if (formatId == -1 && other.formatId == -1)
            return true;

        return formatId == other.formatId
        		&& globalTransactionId.equals(other.globalTransactionId)
        		&& branchQualifier.equals(other.branchQualifier);
    }

    /**
     * Compute the hash code.
     *
     * @return the computed hashcode
     */
    public int hashCode() {
        if (formatId == -1)
            return (-1);
        return formatId ^ globalTransactionId.hashCode() ^ branchQualifier.hashCode();
    }

    /**
     * Return a string representing this XID.
     * <p>
     * This is normally used to display the XID when debugging.
     *
     * @return the string representation of this XID
     */

    public String toString() {
        String gtString = new String(getGlobalTransactionId());
        String brString = new String(getBranchQualifier());
        return new String("{Xid: " + "formatID=" + formatId + ", " + "gtrid[" + globalTransactionId.length + "]=" + gtString + ", " + "brid[" + branchQualifier.length + "]=" + brString + "}");

    }

    /**
     * Obtain the format identifier part of the XID.
     *
     * @return Format identifier. -1 indicates a null XID
     */
    public int getFormatId() {
        return formatId;
    }

    /**
     * Returns the global transaction identifier for this XID.
     *
     * @return the global transaction identifier
     */
    public byte[] getGlobalTransactionId() {
    	// TODO.. may want to compact() first and keep cache that..
        return globalTransactionId.toByteArray();
    }

    /**
     * Returns the branch qualifier for this XID.
     *
     * @return the branch qualifier
     */
    public byte[] getBranchQualifier() {
    	// TODO.. may want to compact() first and keep cache that..
        return branchQualifier.toByteArray();
    }

    ///////////////////////// private methods ////////////////////////////////

    /**
     * Set the branch qualifier for this XID. Note that the branch qualifier has
     * a maximum size.
     *
     * @param branchID
     *            a Byte array containing the branch qualifier to be set. If the
     *            size of the array exceeds MAXBQUALSIZE, only the first
     *            MAXBQUALSIZE elements of qual will be used.
     */
    private void setBranchQualifier(byte[] branchID) {
        if (branchID == null) {
            branchQualifier = EMPTY_BUFFER;
        } else {
            int length = branchID.length > MAXBQUALSIZE ? MAXBQUALSIZE : branchID.length;
            // TODO: Do we really need to copy the bytes??
            branchQualifier = new Buffer(new byte[length]);
            System.arraycopy(branchID, 0, branchQualifier.data, 0, length);
        }
    }

    private void setGlobalTransactionId(byte[] globalTxnID) {
        if (globalTxnID == null) {
            globalTransactionId = EMPTY_BUFFER;
        } else {
        	int length = globalTxnID.length > MAXGTRIDSIZE ? MAXGTRIDSIZE : globalTxnID.length;
            // TODO: Do we really need to copy the bytes??
            globalTransactionId = new Buffer(new byte[length]);
            System.arraycopy(globalTxnID, 0, globalTransactionId.data, 0, length);
        }
    }

    public int getMemorySize() {
        return 4 // formatId
                + 4 // length of globalTxnId
                + globalTransactionId.length // globalTxnId
                + 4 // length of branchId
                + branchQualifier.length; // branchId
    }

    /**
     * Writes this XidImpl's data to the DataOutput destination
     *
     * @param out The DataOutput destination
     */
    public void writebody(DataOutput out) throws IOException {
        out.writeInt(formatId); // format ID

        out.writeInt(globalTransactionId.length); // length of global Txn ID
        out.write(globalTransactionId.data, globalTransactionId.offset, globalTransactionId.length); // global transaction ID
        out.writeInt(branchQualifier.length); // length of branch ID
        out.write(branchQualifier.data, branchQualifier.offset, branchQualifier.length); // branch ID
    }

    /**
     * read xid from an Array and set each fields.
     *
     * @param in
     *            the data input array
     * @throws IOException
     */
    public void readbody(DataInput in) throws IOException {
        formatId = in.readInt();

        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        setGlobalTransactionId(data);

        length = in.readInt();
        data = new byte[length];
        in.readFully(data);
        setBranchQualifier(data);
    }

    /**
     * @param xid
     * @return
     */
    public static Buffer toBuffer(Xid xid) {
        XidImpl x = new XidImpl(xid);
        DataByteArrayOutputStream baos = new DataByteArrayOutputStream(x.getMemorySize());
        try {
            x.writebody(baos);
        } catch (IOException e) {
            //Shouldn't happen:
            throw new RuntimeException(e);
        }
        return baos.toBuffer();

    }
} // class XidImpl
