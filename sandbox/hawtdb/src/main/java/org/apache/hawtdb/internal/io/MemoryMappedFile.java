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
package org.apache.hawtdb.internal.io;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.activemq.util.IOHelper;
import org.fusesource.hawtbuf.Buffer;
import org.apache.hawtdb.api.IOPagingException;

/**
 * Provides Memory Mapped access to a file.  It manages pooling the 
 * direct buffers which mapped to the files.  Multiple direct buffers
 * are used to deal with OS and Java restrictions.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final public class MemoryMappedFile {
	
	private final ByteBufferReleaser BYTE_BUFFER_RELEASER = createByteBufferReleaser();

	private final int bufferSize;
	private final ArrayList<MappedByteBuffer> buffers = new ArrayList<MappedByteBuffer>(10);
	private final FileChannel channel;
	private final FileDescriptor fd;
    private final HashSet<ByteBuffer> bounderyBuffers = new HashSet<ByteBuffer>(10);


	public MemoryMappedFile(File file, int bufferSize) throws IOException {
		this.bufferSize = bufferSize;
		RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
		this.fd = randomAccessFile.getFD();
		this.channel = randomAccessFile.getChannel();
	}

	public void read(long position, byte[] data) throws IOPagingException {
		this.read(position, data, 0, data.length);
	}

	public void read(long position, Buffer data) throws IOPagingException {
		this.read(position, data.data, data.offset, data.length);
	}
	
	public void read(long position, byte[] data, int offset, int length) throws IOPagingException {
		int bufferIndex = (int) (position / bufferSize);
		int bufferOffset = (int) (position % bufferSize);
		ByteBuffer buffer = loadBuffer(bufferIndex);
		buffer = position(buffer, bufferOffset);
		int remaining = buffer.remaining();
		while (length > remaining) {
			buffer.get(data, offset, remaining);
			offset += remaining;
			length -= remaining;
			bufferIndex++;
			buffer = loadBuffer(bufferIndex).duplicate();

		}
		buffer.get(data, offset, length);
	}

	public ByteBuffer read(long position, int length) throws IOPagingException {
		int bufferIndex = (int) (position / bufferSize);
		int bufferOffset = (int) (position % bufferSize);
		ByteBuffer buffer = loadBuffer(bufferIndex);
		buffer = position(buffer, bufferOffset);
		int remaining = buffer.remaining();
		if (length > remaining) {
			// In the case we can't contiguously read the entire buffer.. 
			// fallback to using non-direct buffers..
			byte[] data = new byte[length];
			read(position, data);
			return ByteBuffer.wrap(data);
		} else {
			return (ByteBuffer) buffer.limit(buffer.position()+length);
		}
	}
	
    public ByteBuffer slice(boolean readOnly, long position, int length) {
        int bufferIndex = (int) (position / bufferSize);
        int bufferOffset = (int) (position % bufferSize);
        ByteBuffer buffer = loadBuffer(bufferIndex);
        buffer = position(buffer, bufferOffset);
        int remaining = buffer.remaining();
        if (length > remaining) {
            try {
                buffer = channel.map( readOnly ? MapMode.READ_ONLY : MapMode.READ_WRITE, position, length);
                bounderyBuffers.add(buffer);
                return buffer;
            } catch (IOException e) {
                throw new IOPagingException(e);
            }
        }
        return ((ByteBuffer) buffer.limit(buffer.position()+length)).slice();
    }
    
    public void unslice(ByteBuffer buffer) {
        if( bounderyBuffers.remove(buffer) ) {
            BYTE_BUFFER_RELEASER.release(buffer);
        }
    }

	static public class ChannelTransfer {
		private final FileChannel channel;
		private final long position;
		private final long length;

		public ChannelTransfer(FileChannel channel, long position, long length) {
			this. channel = channel;
			this.position = position;
			this.length = length;
		}

		/**
		 * Writes the transfer to the destinations current file position.
		 * 
		 * @param destination
		 * @throws IOException
		 */
		public void writeTo(FileChannel destination) throws IOException {
			channel.transferTo(position, length, destination);
		}
	}

	public ChannelTransfer readChannelTansfer(int position, int length) throws IOPagingException {
		return new ChannelTransfer(channel, position, length);
	}
	
	public void writeChannelTansfer(long position, ChannelTransfer transfer) throws IOPagingException {
		try {
            channel.position(position);
            transfer.writeTo(channel);
        } catch (IOException e) {
            throw new IOPagingException(e);
        }
	}
	
	public void write(long position, byte[] data) throws IOPagingException {
		this.write(position, data, 0, data.length);
	}

	public void write(long position, Buffer data) throws IOPagingException {
		this.write(position, data.data, data.offset, data.length);
	}

	public void write(long position, ByteBuffer data) throws IOPagingException {
		int bufferIndex = (int) (position / bufferSize);
		int bufferOffset = (int) (position % bufferSize);
		ByteBuffer buffer = loadBuffer(bufferIndex);
		buffer = position(buffer, bufferOffset);
		int remaining = buffer.remaining();
		while (data.remaining() > remaining) {
			int l = data.limit();
			data.limit(data.position()+remaining);
			buffer.put(data);
			data.limit(l);
			bufferIndex++;
			buffer = loadBuffer(bufferIndex).duplicate();
		}
		buffer.put(data);	
	}

	public void write(long position, byte[] data, int offset, int length)
			throws IOPagingException {
		int bufferIndex = (int) (position / bufferSize);
		int bufferOffset = (int) (position % bufferSize);
		ByteBuffer buffer = loadBuffer(bufferIndex);
		buffer = position(buffer, bufferOffset);
		int remaining = buffer.remaining();
		while (length > remaining) {
			buffer.put(data, offset, remaining);
			offset += remaining;
			length -= remaining;
			bufferIndex++;
			buffer = loadBuffer(bufferIndex).duplicate();

		}
		buffer.put(data, offset, length);
	}

	private ByteBuffer position(ByteBuffer buffer, int offset) {
		return (ByteBuffer) buffer.duplicate().position(offset);
	}

	private MappedByteBuffer loadBuffer(int index) throws IOPagingException {
		while (index >= buffers.size()) {
			buffers.add(null);
		}
		MappedByteBuffer buffer = buffers.get(index);
		if (buffer == null) {
			try {
                long position = ((long)index)*bufferSize;
                buffer = channel.map(MapMode.READ_WRITE, position, bufferSize);
            } catch (IllegalArgumentException e) {
                throw new IOPagingException(e);
            } catch (IOException e) {
                throw new IOPagingException(e);
            }
			buffers.set(index, buffer);
		}
		return buffer;
	}

	public void sync() throws IOPagingException {
		for (MappedByteBuffer buffer : buffers) {
			if (buffer != null) {
				buffer.force();
			}
		}
        try {
            IOHelper.sync(fd);
        } catch (IOException e) {
            throw new IOPagingException(e);
        }
	}
	
	public void close() throws IOPagingException {
		sync();
		for (MappedByteBuffer buffer : buffers) {
			if (buffer != null) {
				BYTE_BUFFER_RELEASER.release(buffer);
			}
		}
		buffers.clear();
		try {
            channel.close();
        } catch (IOException e) {
            throw new IOPagingException(e);
        }
	}

	private static interface ByteBufferReleaser {
		public void release(ByteBuffer buffer);
	}
	
	static private ByteBufferReleaser createByteBufferReleaser() {
		
		// Try to drill into the java.nio.DirectBuffer internals...
		final Method[] cleanerMethods = AccessController.doPrivileged(new PrivilegedAction<Method[]>() {
			public Method[] run() {
				try {
					ByteBuffer buffer = ByteBuffer.allocateDirect(1);
					Class<?> bufferClazz = buffer.getClass();
					Method cleanerMethod = bufferClazz.getMethod("cleaner", new Class[0]);
					cleanerMethod.setAccessible(true);
					Method cleanMethod = cleanerMethod.getReturnType().getMethod("clean");
					return new Method[]{cleanerMethod, cleanMethod};
				} catch (Exception e) {
					return null;
				}
			}
		});
		
		// Yay, we can actually release the buffers.
		if( cleanerMethods !=null ) {
			return new ByteBufferReleaser() {
				public void release(ByteBuffer buffer) {
					try {
						Object cleaner = cleanerMethods[0].invoke(buffer);
						if( cleaner!=null ) {
							cleanerMethods[1].invoke(cleaner);
						}
					} catch (Throwable e) {
						e.printStackTrace();
					}
				}
			};
		}
		
		// We can't really release the buffers.. Good Luck!
		return new ByteBufferReleaser() {
			public void release(ByteBuffer buffer) {
			}
		};
	}

}
