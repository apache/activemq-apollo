package org.apache.activemq.util.buffer;





final public class AsciiBuffer extends Buffer {

    private int hashCode;
    private String value;
    
    public AsciiBuffer(Buffer other) {
        super(other);
    }

    public AsciiBuffer(byte[] data, int offset, int length) {
        super(data, offset, length);
    }

    public AsciiBuffer(byte[] data) {
        super(data);
    }

    public AsciiBuffer(String value) {
        super(encode(value));
        this.value = value;
    }

    public static AsciiBuffer ascii(String value) {
        if( value==null ) {
            return null;
        }
        return new AsciiBuffer(value);
    }
    
    public static AsciiBuffer ascii(Buffer buffer) {
        if( buffer==null ) {
            return null;
        }
        if( buffer.getClass() == AsciiBuffer.class ) {
            return (AsciiBuffer) buffer;
        }
        return new AsciiBuffer(buffer);
    }    
    
    public AsciiBuffer compact() {
        if (length != data.length) {
            return new AsciiBuffer(toByteArray());
        }
        return this;
    }

    @Override
    protected AsciiBuffer createBuffer(byte[] data, int offset, int length) {
		return new AsciiBuffer(data, offset, length);
	}
    
    public String toString()
    {
    	if( value == null ) {
    		value = decode(this); 
    	}
        return value; 
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
