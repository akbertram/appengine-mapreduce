package com.google.appengine.tools.mapreduce.fs;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import com.google.appengine.api.blobstore.BlobInfo;
import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;

public class AppEngineInputStream extends InputStream implements Seekable, PositionedReadable {
  
  private final BlobstoreService blobstore;
  private final BlobKey key;
  
  /**
   * Total size of the file
   */
  private long size;
    
  private byte[] buffer = null;
  private long bufferStart = -1;
  private int bufferSize;
  private long pos = 0;
  
  public AppEngineInputStream(BlobstoreService blobstore, BlobKey key, int bufferSize) {
    super();
    this.blobstore = blobstore;
    this.key = key;
    
    BlobInfoFactory blobInfoFactory = new BlobInfoFactory();
    BlobInfo blobInfo = blobInfoFactory.loadBlobInfo(key);

    size = blobInfo.getSize();
    
    this.bufferSize = bufferSize;
    if(this.bufferSize > BlobstoreService.MAX_BLOB_FETCH_SIZE) {
      this.bufferSize = BlobstoreService.MAX_BLOB_FETCH_SIZE;
    }
    buffer = null;
  }


  @Override
  public int read() throws IOException {
    if(isEOF())
      return -1;
    if(noBufferRemaining()) {
      fillBuffer();
    }
    byte b = buffer[ (int)( (pos++) - bufferStart) ];
    return (int) b & 0xFF;
  }

  @Override
  public long skip(long n) throws IOException {
    long bytesUntilEnd = size-pos;
    long bytesToSkip = Math.min(n, bytesUntilEnd);
    pos+=bytesToSkip;
    return bytesToSkip;
  }

  @Override
  public int read(byte[] b, int offset, int bytesRequested) throws IOException {
    if(isEOF())
      return -1;
    if(noBufferRemaining())
      fillBuffer();

    // only read to the end of our buffer.
    // the next call will result in a new fetchData call
    long bytesRemainingInBuffer = bufferStart + buffer.length - pos;
    int bytesToRead = (int) Math.min(bytesRequested, bytesRemainingInBuffer);

    System.arraycopy(buffer, (int)(pos-bufferStart), b, offset, bytesToRead);

    pos+=bytesToRead;

    return bytesToRead;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  private void fillBuffer() {
    buffer = blobstore.fetchData(key, pos, pos + Math.min(bufferSize, size-pos));
    bufferStart = pos;
  }

  private boolean noBufferRemaining() {
    return buffer == null || (bufferStart+buffer.length) <= pos;
  }

  private boolean isEOF() {
    return pos >= size;
  }


  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    if(position >= size) {
      return -1;
    }
    if(length > BlobstoreService.MAX_BLOB_FETCH_SIZE) {
      length = BlobstoreService.MAX_BLOB_FETCH_SIZE;
    }
    
    byte[] bytesRead = blobstore.fetchData(key, position, position+length);
    System.arraycopy(bytesRead, 0, buffer, offset, length);
    
    return bytesRead.length;
  }


  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {

    int bytesRead;
    while((bytesRead=read(position,buffer,offset,length)) != -1) {
      offset+= bytesRead;
      length-= bytesRead;
    }
    
  }


  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public void seek(long pos) throws IOException {
    this.pos = pos;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new UnsupportedOperationException("i don't know what this does!!");
  }
}
