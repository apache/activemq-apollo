package org.apache.activemq.protobuf;


final public class AsciiBuffer extends Buffer {

    private int hashCode;

    public AsciiBuffer(Buffer other) {
        super(other);
    }

    public AsciiBuffer(byte[] data, int offset, int length) {
        super(data, offset, length);
    }

    public AsciiBuffer(byte[] data) {
        super(data);
    }

    public AsciiBuffer(String input) {
        super(encode(input));
    }

    public AsciiBuffer compact() {
        if (length != data.length) {
            return new AsciiBuffer(toByteArray());
        }
        return this;
    }

    public String toString()
    {
        return decode(this);
    }

    @Override
    public boolean equals(Object obj) {
        if( obj==this )
            return true;
         
         if( obj==null || obj.getClass()!=AsciiBuffer.class )
            return false;
         
         return equals((Buffer)obj);
    }
    
    @Override
    public int hashCode() {
        if( hashCode==0 ) {
            hashCode = super.hashCode();;
        }
        return hashCode;
    }
    
    static public byte[] encode(String value)
    {
        int size = value.length();
        byte rc[] = new byte[size];
        for( int i=0; i < size; i++ ) {
            rc[i] = (byte)(value.charAt(i)&0xFF);
        }
        return rc;
    }
    static public String decode(Buffer value)
    {
        int size = value.getLength();
        char rc[] = new char[size];
        for( int i=0; i < size; i++ ) {
            rc[i] = (char)(value.byteAt(i) & 0xFF );
        }
        return new String(rc);
    }

    
}
