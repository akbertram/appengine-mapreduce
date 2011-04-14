package com.google.appengine.tools.mapreduce.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

import com.google.appengine.api.files.FileWriteChannel;

public class AppEngineOutputStream extends OutputStream {

  private final FileWriteChannel channel;
  private final OutputStream output;

  public AppEngineOutputStream(FileWriteChannel channel) {
    super();
    this.channel = channel;
    this.output = Channels.newOutputStream(channel);
  }
  
  @Override
  public void write(int b) throws IOException {
    output.write(b);
  }


  @Override
  public void close() throws IOException {
    output.close();
    channel.close();
  }


  @Override
  public void flush() throws IOException {
    output.flush();
  }


  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    output.write(b,off,len);
  }


  @Override
  public void write(byte[] b) throws IOException {
    output.write(b);
  }

}
