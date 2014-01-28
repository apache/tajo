/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.v2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ScheduledInputStream extends InputStream implements Seekable, Closeable, DataInput {
  private static final Log LOG = LogFactory.getLog(ScheduledInputStream.class);

	private FSDataInputStream originStream;

  private int currentScanIndex;

  private Queue<ScanData> dataQueue = new LinkedList<ScanData>();

  private ScanData currentScanData;

  private AtomicBoolean closed = new AtomicBoolean(false);

  private boolean eof = false;

  private long pos;

  private AtomicInteger avaliableSize = new AtomicInteger(0);

  private long fileLen;

  private long startOffset;

  private long length;

  private long endOffset;

  private boolean endOfStream = false;

  private Path file;

  private byte readLongBuffer[] = new byte[8];

  private AtomicLong totalReadBytesForFetch = new AtomicLong(0);

  private AtomicLong totalReadBytesFromDisk = new AtomicLong(0);

	public ScheduledInputStream(Path file, FSDataInputStream originStream,
                              long startOffset, long length, long fileLen) throws IOException {
		this.originStream = originStream;
		this.startOffset = startOffset;
		this.length = length;
		this.endOffset = startOffset + length;
		this.fileLen = fileLen;
    this.file = file;
		this.pos = this.originStream.getPos();

    LOG.info("Open:" + toString());
	}

	public int getAvaliableSize() {
		return avaliableSize.get();
	}

  public String toString() {
    return file.getName() + ":" + startOffset + ":" + length;
  }
	public boolean readNext(int length) throws IOException {
		return readNext(length, false);
	}
	
	public boolean readNext(int length, boolean ignoreEOS) throws IOException {
    synchronized(dataQueue) {
      if(closed.get() || (!ignoreEOS && endOfStream)) {
        return false;
      }
      int bufLength = ignoreEOS ? length : (int)Math.min(length,  endOffset - originStream.getPos());
      bufLength = (int)Math.min(bufLength, fileLen - originStream.getPos());
      if(bufLength == 0) {
        return false;
      }
			byte[] buf = new byte[bufLength];

      try {
        originStream.readFully(buf);
      } catch (EOFException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (Exception e) {
        throw new IOException(e.getMessage(), e);
      }

      if(originStream.getPos() == fileLen) {
        LOG.info("EOF:" + toString());
        eof = true;
      }
      if(!ignoreEOS && originStream.getPos() >= endOffset) {
        LOG.info("EndOfStream:" + toString());
        endOfStream = true;
      }

      if(currentScanData == null) {
        currentScanData = new ScanData(buf, bufLength);
        currentScanIndex = 0;
      } else {
        dataQueue.offer(new ScanData(buf, bufLength));
      }

      avaliableSize.addAndGet(bufLength);

      if(LOG.isDebugEnabled()) {
        LOG.debug("Add DataQueue: queue=" + dataQueue.size() +
          ", avaliable Size=" + avaliableSize.get() + ", pos=" + getPos() +
          ", originPos=" + originStream.getPos() + ",endOfStream=" + endOfStream +
          ", bufLength=" + bufLength + ",ignoreEOS=" + ignoreEOS);
      }

      totalReadBytesFromDisk.addAndGet(bufLength);
      dataQueue.notifyAll();
    }
    return !eof;
	}
	
	static class ScanData {
		byte[] data;
		int length;
		public ScanData(byte[] buf, int length) {
			this.data = buf;
			this.length = length;
		}
		
		@Override
		public String toString() {
			return "length=" + length;
		}
	}

	@Override
	public void seek(long pos) throws IOException {
		synchronized(dataQueue) {
			dataQueue.clear();
			currentScanData = null;
			currentScanIndex = 0;
			avaliableSize.set(0);
      originStream.seek(pos);
      this.pos = pos;
    }
	}

	@Override
	public long getPos() throws IOException {
		return this.pos;
	}

	public long getOriginStreamPos() {
		try {
			return this.originStream.getPos();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {
		synchronized(dataQueue) {
			dataQueue.clear();
			currentScanData = null;
			currentScanIndex = 0;
			avaliableSize.set(0);
      boolean result = originStream.seekToNewSource(targetPos);

      this.pos = originStream.getPos();
      return result;
		}
	}

	@Override
	public int read() throws IOException {
		if(noMoreData()) {
			return -1;
		}
		if(currentScanData == null || currentScanIndex >= currentScanData.length) {
			synchronized(dataQueue) {
				if(dataQueue.isEmpty()) {
					if(endOfStream) {
						readNext(64 * 1024, true);
					} else {
						try {
							dataQueue.wait();
							if(eof && dataQueue.isEmpty() && currentScanIndex > 0) {
								//no more data
								return -1;
							}
						} catch (InterruptedException e) {
						}
					}
				}
				if(!dataQueue.isEmpty() && currentScanIndex > 0) {
					currentScanData = dataQueue.poll();
					currentScanIndex = 0;
				}
			}
		} 
		
		this.pos++;
		avaliableSize.decrementAndGet();
    totalReadBytesForFetch.incrementAndGet();

		return currentScanData.data[currentScanIndex++] & 0xff;
	}
	
	private boolean noMoreData() {
		return closed.get();
	}
	
	public int read(byte b[], int off, int len) throws IOException {
		if(noMoreData()) {
			return -1;
		}
		if (b == null) {
		    throw new NullPointerException();
		} else if (off < 0 || len < 0 || len > b.length - off) {
		    throw new IndexOutOfBoundsException();
		} else if (len == 0) {
		    return 0;
		}
		if(currentScanData == null) {
			synchronized(dataQueue) {
				if(dataQueue.isEmpty()) {
					if(endOfStream) {
						readNext(64 * 1024, true);
					} else {
						try {
							dataQueue.wait();
							if(noMoreData()) {
								return -1;
							}
						} catch (InterruptedException e) {
						}
					}
				}
				if(!dataQueue.isEmpty() && currentScanIndex > 0) {
					currentScanData = dataQueue.poll();
					currentScanIndex = 0;
				}
			}
		} 
		
		int numRemainBytes = currentScanData.length - currentScanIndex;
		if(numRemainBytes > len) {
			System.arraycopy(currentScanData.data, currentScanIndex, b, off, len);
			currentScanIndex += len;
			avaliableSize.addAndGet(0 - len);
			pos += len;

      totalReadBytesForFetch.addAndGet(len);
			return len;
		} else {
			int offset = off;
			int length = 0;
			int numCopyBytes = numRemainBytes;
			while(true) {
				synchronized(dataQueue) {
					if(numCopyBytes == 0 && eof && dataQueue.isEmpty()) {
						return -1;
					}
				}
				System.arraycopy(currentScanData.data, currentScanIndex, b, offset, numCopyBytes);
				currentScanIndex += numCopyBytes;
				offset += numCopyBytes;
				length += numCopyBytes;
				if(length >= len) {
					break;
				}
				synchronized(dataQueue) {
					if(dataQueue.isEmpty()) {
						if(eof) {
							break;
						}
						if(endOfStream) {
							readNext(64 * 1024, true);
						} else {
							try {
								dataQueue.wait();
							} catch (InterruptedException e) {
							}
						}
					}
					if(eof && dataQueue.isEmpty()) {
						break;
					}
					if(!dataQueue.isEmpty() && currentScanIndex > 0) {
						currentScanData = dataQueue.poll();
						currentScanIndex = 0;
					}
					if(currentScanData == null) {
						break;
					}
				}
        if(currentScanData.length >= (len - length)) {
          numCopyBytes = (len - length);
        } else {
          numCopyBytes = currentScanData.length;
        }
			}  //end of while
			this.pos += length;
			avaliableSize.addAndGet(0 - length);

      totalReadBytesForFetch.addAndGet(length);
			return length;
		}
	}

  public long getTotalReadBytesForFetch() {
    return totalReadBytesForFetch.get();
  }

  public long getTotalReadBytesFromDisk() {
    return totalReadBytesFromDisk.get();
  }

	@Override
	public void close() throws IOException {
    LOG.info("Close:" + toString());
		synchronized(dataQueue) {
			if(closed.get()) {
				return;
			}
			closed.set(true);
			originStream.close();
			dataQueue.clear();
			currentScanIndex = 0;
			super.close();
		}
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		if (len < 0) {
		    throw new IndexOutOfBoundsException();
		}
		int n = 0;
		while (n < len) {
		    int count = read(b, off + n, len - n);
		    if (count < 0) {
		    	throw new EOFException();
		    }
		    n += count;
		}
	}

	@Override
	public int skipBytes(int bytes) throws IOException {
		int skipTotal = 0;
		int currentPos = 0;

		while ((skipTotal<bytes) && ((currentPos = (int)skip(bytes-skipTotal)) > 0)) {
      skipTotal += currentPos;
		}

		return skipTotal;
	}

	@Override
	public boolean readBoolean() throws IOException {
		int val = read();
		if (val < 0) {
		    throw new EOFException();
    }
		return (val != 0);
	}

	@Override
	public byte readByte() throws IOException {
		int val = read();
		if (val < 0) {
		    throw new EOFException();
    }
		return (byte)(val);
	}

	@Override
	public int readUnsignedByte() throws IOException {
		int val = read();
		if (val < 0) {
		    throw new EOFException();
    }
		return val;
	}

	@Override
	public short readShort() throws IOException {
    int val1 = read();
    int val2 = read();
    if ((val1 | val2) < 0) {
        throw new EOFException();
    }
    return (short)((val1 << 8) + (val2 << 0));
	}

	@Override
	public int readUnsignedShort() throws IOException {
    int val1 = read();
    int val2 = read();
    if ((val1 | val2) < 0) {
        throw new EOFException();
    }
    return (val1 << 8) + (val2 << 0);
	}

	@Override
	public char readChar() throws IOException {
    int val1 = read();
    int val2 = read();
    if ((val1 | val2) < 0) {
        throw new EOFException();
    }
    return (char)((val1 << 8) + (val2 << 0));
	}

	@Override
	public int readInt() throws IOException {
    int val1 = read();
    int val2 = read();
    int val3 = read();
    int val4 = read();
    if ((val1 | val2 | val3 | val4) < 0) {
        throw new EOFException();
    }
    return ((val1 << 24) + (val2 << 16) + (val3 << 8) + (val4 << 0));
	}

	@Override
	public long readLong() throws IOException {
    readFully(readLongBuffer, 0, 8);
    return  (((long) readLongBuffer[0] << 56) +
            ((long)(readLongBuffer[1] & 255) << 48) +
		        ((long)(readLongBuffer[2] & 255) << 40) +
            ((long)(readLongBuffer[3] & 255) << 32) +
            ((long)(readLongBuffer[4] & 255) << 24) +
            ((readLongBuffer[5] & 255) << 16) +
            ((readLongBuffer[6] & 255) <<  8) +
            ((readLongBuffer[7] & 255) <<  0));
	}

	@Override
	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	@Override
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public String readLine() throws IOException {
		throw new IOException("Unsupported operation: readLine");
	}

	@Override
	public String readUTF() throws IOException {
		throw new IOException("Unsupported operation: readUTF");
	}

	public boolean isEOF() {
		return eof;
	}

	public boolean isEndOfStream() {
		return endOfStream;
	}

  public void reset() {
    synchronized(dataQueue) {
      endOfStream = false;
      eof = false;
      closed.set(false);
      dataQueue.clear();
      currentScanIndex = 0;
      currentScanData = null;
    }
  }
}
