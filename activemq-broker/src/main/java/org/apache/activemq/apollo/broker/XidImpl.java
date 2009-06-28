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

/**
 * An implementation of JTA transaction idenifier (javax.transaction.xa.Xid).
 * This is SonicMQ internal Xid. Any external Xid object will be converted to
 * this class.
 */
public class XidImpl implements Xid, Cloneable, java.io.Serializable {

    //fix bug #8334
    static final long serialVersionUID = -5363901495878210611L;

    // The format identifier for the XID. A value of -1 indicates the NULLXID
    private int m_formatID = -1; // default format

    private byte m_gtrid[];
    // The number of bytes in the global transaction identfier
    private int m_gtridLength; // Value from 1 through MAXGTRIDSIZE

    private byte m_bqual[];
    // The number of bytes in the branch qualifier
    private int m_bqualLength; // Value from 1 through MAXBQUALSIZE

    /////////////////////////////// Constructors /////////////////////////////
    /**
     * Constructs a new null XID.
     * <p>
     * After construction the data within the XID should be initialized.
     */
    public XidImpl() {
        this(-1, null, null);
    }

    public XidImpl(int formatID, byte[] globalTxnID, byte[] branchID) {
        m_formatID = formatID;
        setGlobalTransactionId(globalTxnID);
        setBranchQualifier(branchID);
    }

    /**
     * Initialize an XID using another XID as the source of data.
     * 
     * @param from
     *            the XID to initialize this XID from
     */
    public XidImpl(Xid from) {

        if ((from == null) || (from.getFormatId() == -1)) {
            m_formatID = -1;
            setGlobalTransactionId(null);
            setBranchQualifier(null);
        } else {
            m_formatID = from.getFormatId();
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
        Xid other;

        if (!(o instanceof Xid))
            return false;

        other = (Xid) o;

        if (m_formatID == -1 && other.getFormatId() == -1)
            return true;

        if (m_formatID != other.getFormatId() || m_gtridLength != other.getGlobalTransactionId().length || m_bqualLength != other.getBranchQualifier().length) {
            return false;
        }

        return isEqualGtrid(other) && isEqualBranchQualifier(other.getBranchQualifier());

    }

    /**
     * Compute the hash code.
     * 
     * @return the computed hashcode
     */

    public int hashCode() {

        if (m_formatID == -1)
            return (-1);

        return m_formatID + m_gtridLength - m_bqualLength;

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
        return new String("{Xid: " + "formatID=" + m_formatID + ", " + "gtrid[" + m_gtridLength + "]=" + gtString + ", " + "brid[" + m_bqualLength + "]=" + brString + "}");

    }

    /**
     * Obtain the format identifier part of the XID.
     * 
     * @return Format identifier. -1 indicates a null XID
     */
    public int getFormatId() {
        return m_formatID;
    }

    /**
     * Returns the global transaction identifier for this XID.
     * 
     * @return the global transaction identifier
     */
    public byte[] getGlobalTransactionId() {
        return m_gtrid;
    }

    /**
     * Returns the branch qualifier for this XID.
     * 
     * @return the branch qualifier
     */
    public byte[] getBranchQualifier() {
        return m_bqual;
    }

    ///////////////////////// private methods ////////////////////////////////

    /**
     * Set the branch qualifier for this XID. Note that the branch qualifier has
     * a maximum size.
     * 
     * @param qual
     *            a Byte array containing the branch qualifier to be set. If the
     *            size of the array exceeds MAXBQUALSIZE, only the first
     *            MAXBQUALSIZE elements of qual will be used.
     */
    private void setBranchQualifier(byte[] branchID) {
        if (branchID == null) {
            m_bqualLength = 0;
            m_bqual = new byte[m_bqualLength];
        } else {
            m_bqualLength = branchID.length > MAXBQUALSIZE ? MAXBQUALSIZE : branchID.length;
            m_bqual = new byte[m_bqualLength];
            System.arraycopy(branchID, 0, m_bqual, 0, m_bqualLength);
        }
    }

    private void setGlobalTransactionId(byte[] globalTxnID) {
        if (globalTxnID == null) {
            m_gtridLength = 0;
            m_gtrid = new byte[m_gtridLength];
        } else {
            m_gtridLength = globalTxnID.length > MAXGTRIDSIZE ? MAXGTRIDSIZE : globalTxnID.length;
            m_gtrid = new byte[m_gtridLength];
            System.arraycopy(globalTxnID, 0, m_gtrid, 0, m_gtridLength);
        }
    }

    /**
     * Return whether the Gtrid of this is equal to the Gtrid of xid
     */
    private boolean isEqualGtrid(Xid xid) {
        byte[] xidGtrid = xid.getGlobalTransactionId();

        if (getGlobalTransactionId() == null && xidGtrid == null)
            return true;
        if (getGlobalTransactionId() == null)
            return false;
        if (xidGtrid == null)
            return false;

        if (m_gtridLength != xidGtrid.length) {
            return false;
        }

        for (int i = 0; i < m_gtridLength; i++) {
            if (m_gtrid[i] != xidGtrid[i])
                return false;
        }
        return true;
    }

    /**
     * Determine if an array of bytes equals the branch qualifier
     * 
     * @return true if equal
     */
    private boolean isEqualBranchQualifier(byte[] data) {

        int L = data.length > MAXBQUALSIZE ? MAXBQUALSIZE : data.length;

        if (L != m_bqualLength)
            return false;

        for (int i = 0; i < m_bqualLength; i++) {
            if (data[i] != m_bqual[i])
                return false;
        }

        return true;
    }

    public int getMemorySize() {
        return 4 // formatId
                + 4 // length of globalTxnId
                + m_gtridLength // globalTxnId
                + 4 // length of branchId
                + m_bqualLength; // branchId
    }

    /**
     * Writes this XidImpl's data to the DataOutput destination
     * 
     * @param out
     *            The DataOutput destination
     * @param maxbytes
     *            Maximum number of bytes that may be written to the destination
     * 
     * @exception ELogEventTooLong
     *                The data could not be written without exceeding the
     *                maxbytes parameter. The data may have been partially
     *                written.
     */
    public void writebody(DataOutput out) throws IOException {
        out.writeInt(m_formatID); // format ID

        out.writeInt(m_gtridLength); // length of global Txn ID
        out.write(getGlobalTransactionId(), 0, m_gtridLength); // global transaction ID
        out.writeInt(m_bqualLength); // length of branch ID
        out.write(getBranchQualifier(), 0, m_bqualLength); // branch ID
    }

    /**
     * read xid from an Array and set each fields.
     * 
     * @param in
     *            the data input array
     * @throws IOException
     */
    public void readbody(DataInput in) throws IOException {
        m_formatID = in.readInt();
        int gtidLen = in.readInt();
        byte[] globalTxnId = new byte[gtidLen];
        in.readFully(globalTxnId, 0, gtidLen);

        int brlen = in.readInt();
        byte[] branchId = new byte[brlen];
        in.readFully(branchId, 0, brlen);

        setGlobalTransactionId(globalTxnId);
        setBranchQualifier(branchId);
    }
} // class XidImpl
