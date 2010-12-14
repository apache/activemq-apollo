package org.apache.activemq.apollo.transport.tcp;

import org.apache.activemq.apollo.util.ApolloThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;

/**
 * An SSL Transport for secure communications.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class SslTransport extends TcpTransport {

    private static final Logger LOG = LoggerFactory.getLogger(TcpTransport.class);
    private SSLContext sslContext;
    private SSLEngine engine;

    private ByteBuffer readBuffer;
    private boolean readUnderflow;

    private ByteBuffer writeBuffer;
    private boolean writeFlushing;

    private ByteBuffer readOverflowBuffer;

    public void setSSLContext(SSLContext ctx) {
        this.sslContext = ctx;
    }

    class SSLChannel implements ReadableByteChannel, WritableByteChannel {

        public int write(ByteBuffer plain) throws IOException {
            return secure_write(plain);
        }

        public int read(ByteBuffer plain) throws IOException {
            return secure_read(plain);
        }

        public boolean isOpen() {
            return channel.isOpen();
        }

        public void close() throws IOException {
            channel.close();
        }
    }

    public SSLSession getSSLSession() {
        return engine==null ? null : engine.getSession();
    }

    public X509Certificate[] getPeerX509Certificates() {
    	if( engine==null ) {
            return null;
        }
        try {
            ArrayList<X509Certificate> rc = new ArrayList<X509Certificate>();
            for( Certificate c:engine.getSession().getPeerCertificates() ) {
                if(c instanceof X509Certificate) {
                    rc.add((X509Certificate) c);
                }
            }
            return rc.toArray(new X509Certificate[rc.size()]);
        } catch (SSLPeerUnverifiedException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected void initializeCodec() {
        SSLChannel channel = new SSLChannel();
        codec.setReadableByteChannel(channel);
        codec.setWritableByteChannel(channel);
    }


    @Override
    public void connecting(URI remoteLocation, URI localLocation) throws Exception {
        assert engine == null;
        engine = sslContext.createSSLEngine();
        engine.setUseClientMode(true);
        super.connecting(remoteLocation, localLocation);
    }

    @Override
    public void connected(SocketChannel channel) throws Exception {
        if (engine == null) {
            engine = sslContext.createSSLEngine();
            engine.setUseClientMode(false);
            engine.setWantClientAuth(true);
        }
        SSLSession session = engine.getSession();
        readBuffer = ByteBuffer.allocateDirect(session.getPacketBufferSize());
        readBuffer.flip();
        writeBuffer = ByteBuffer.allocateDirect(session.getPacketBufferSize());

        super.connected(channel);


    }

    @Override
    protected void onConnected() throws IOException {
        super.onConnected();
        engine.setWantClientAuth(true);
        engine.beginHandshake();
        handshake_done();
    }

    @Override
    protected void drainOutbound() {
        if ( handshake_done() ) {
            super.drainOutbound();
        }
    }

    @Override
    protected void drainInbound() {
        if ( handshake_done() ) {
            super.drainInbound();
        }
    }

    /**
     * @return true if fully flushed.
     * @throws IOException
     */
    protected boolean flush() throws IOException {
        while (true) {
            if(writeFlushing) {
                channel.write(writeBuffer);
                if( !writeBuffer.hasRemaining() ) {
                    writeBuffer.clear();
                    writeFlushing = false;
                    return true;
                } else {
                    return false;
                }
            } else {
                if( writeBuffer.position()!=0 ) {
                    writeBuffer.flip();
                    writeFlushing = true;
                } else {
                    return true;
                }
            }
        }
    }

    private int secure_write(ByteBuffer plain) throws IOException {
        if( !flush() ) {
            // can't write anymore until the write_secured_buffer gets fully flushed out..
            return 0;
        }
        int rc = 0;
        while ( plain.hasRemaining() || engine.getHandshakeStatus()==NEED_WRAP ) {
            SSLEngineResult result = engine.wrap(plain, writeBuffer);
            assert result.getStatus()!= BUFFER_OVERFLOW;
            rc += result.bytesConsumed();
            if( !flush() ) {
                break;
            }
        }
        return rc;
    }

    private int secure_read(ByteBuffer plain) throws IOException {
        int rc=0;
        while ( plain.hasRemaining() || engine.getHandshakeStatus() == NEED_UNWRAP ) {
            if( readOverflowBuffer !=null ) {
                // lets drain the overflow buffer before trying to suck down anymore
                // network bytes.
                int size = Math.min(plain.remaining(), readOverflowBuffer.remaining());
                plain.put(readOverflowBuffer.array(), 0, size);
                readOverflowBuffer.position(readOverflowBuffer.position()+size);
                if( !readOverflowBuffer.hasRemaining() ) {
                    readOverflowBuffer = null;
                }
                rc += size;
            } else if( readUnderflow ) {
                int count = channel.read(readBuffer);
                if( count == -1 ) {  // peer closed socket.
                    if (rc==0) {
                        engine.closeInbound();
                        return -1;
                    } else {
                        return rc;
                    }
                }
                if( count==0 ) {  // no data available right now.
                    return rc;
                }
                // read in some more data, perhaps now we can unwrap.
                readUnderflow = false;
                readBuffer.flip();
            } else {
                SSLEngineResult result = engine.unwrap(readBuffer, plain);
                rc += result.bytesProduced();
                if( result.getStatus() == BUFFER_OVERFLOW ) {
                    readOverflowBuffer = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
                    result = engine.unwrap(readBuffer, readOverflowBuffer);
                    if( readOverflowBuffer.position()==0 ) {
                        readOverflowBuffer = null;
                    } else {
                        readOverflowBuffer.flip();
                    }
                }
                switch( result.getStatus() ) {
                    case CLOSED:
                        if (rc==0) {
                            engine.closeInbound();
                            return -1;
                        } else {
                            return rc;
                        }
                    case OK:
                        break;
                    case BUFFER_UNDERFLOW:
                        readBuffer.compact();
                        readUnderflow = true;
                        break;
                    case BUFFER_OVERFLOW:
                        throw new AssertionError("Unexpected case.");
                }
            }
        }
        return rc;
    }

    public boolean handshake_done() {
        while (true) {
            switch (engine.getHandshakeStatus()) {
                case NEED_TASK:
                    final Runnable task = engine.getDelegatedTask();
                    if( task!=null ) {
                        ApolloThreadPool.INSTANCE.execute(new Runnable() {
                            public void run() {
                                task.run();
                                dispatchQueue.execute(new Runnable() {
                                    public void run() {
                                        if (isConnected()) {
                                            handshake_done();
                                        }
                                    }
                                });
                            }
                        });
                        return false;
                    }
                    break;

                case NEED_WRAP:
                    try {
                        secure_write(ByteBuffer.allocate(0));
                        if( writeFlushing && writeSource.isSuspended() ) {
                            writeSource.resume();
                            return false;
                        }
                    } catch(IOException e) {
                        onTransportFailure(e);
                    }
                    break;

                case NEED_UNWRAP:
                    try {
                        secure_read(ByteBuffer.allocate(0));
                        if( readUnderflow && readSource.isSuspended() ) {
                            readSource.resume();
                            return false;
                        }
                    } catch(IOException e) {
                        onTransportFailure(e);
                        return true;
                    }
                    break;

                case FINISHED:

                case NOT_HANDSHAKING:
                    return true;

                default:
                    SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
                    System.out.println("Unexpected ssl engine handshake status: "+ status);
            }
        }
    }

}


