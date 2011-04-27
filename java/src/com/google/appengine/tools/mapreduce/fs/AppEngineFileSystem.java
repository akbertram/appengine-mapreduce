package com.google.appengine.tools.mapreduce.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.google.appengine.api.blobstore.BlobInfo;
import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.api.files.FinalizationException;

public class AppEngineFileSystem extends FileSystem {
  
  private static final String FILE_DESC_KIND = "MapReduceFs";
  private DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
  private FileService fileService = FileServiceFactory.getFileService();
  private BlobstoreService blobstore = BlobstoreServiceFactory.getBlobstoreService();
  private BlobInfoFactory blobInfoFactory = new BlobInfoFactory(datastore);
  
  private Path workingDirectory;
  
  public static final String SCHEME = "appengine";
  public static final String AUTHORITY = "blobstore";
  
  private static final int BLOCK_REPLICATION = 1;
  private static final int BLOCK_SIZE = BlobstoreService.MAX_BLOB_FETCH_SIZE;
  
  
  
  public AppEngineFileSystem() {
    super();
    workingDirectory = new Path(SCHEME, AUTHORITY, "/");
  }

  @Override
  public URI getUri() {
    try {
      return new URI(SCHEME, AUTHORITY, "/", null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    } 
  }

  private AppEngineFile fileForPath(Path f) throws FileNotFoundException {
    try {
      Entity entity = datastore.get(keyForPath(f));
      return new AppEngineFile((String) entity.getProperty("fullPath"));
    } catch (EntityNotFoundException e) {
      throw new FileNotFoundException("Cannot find entity entry for '" + f.toString() + "'");
    }
  }
  
  private BlobKey blobKeyForPath(Path f) throws FileNotFoundException {
    return fileService.getBlobKey( fileForPath(f) );
  }
  
  private FileDescriptor descForPath(Path f) throws FileNotFoundException {
    f = makeAbsolute(f);
    if(f.getParent() == null) {
      return newDirectory(f);
    }
    try {
      return new FileDescriptor( datastore.get(keyForPath(f)) );
    } catch (EntityNotFoundException e) {
      throw new FileNotFoundException(f.toString());
    } 
  }

  private Path makeAbsolute(Path f) {
    if(!f.isAbsolute()) {
      f = new Path(workingDirectory, f);
    }
    return f;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {   
    return new FSDataInputStream(
        new AppEngineInputStream(blobstore, descForPath(f).getBlobKey(), bufferSize) );
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    
    path = makeAbsolute(path);
    
    if(!overwrite && exists(path)) {
        throw new IOException("File at path '" + path.toString() + "' already exists");
    }
    
    // check for parent folder
    Path parent = path.getParent();
    if(parent != null && !mkdirs(parent)) {
      throw new IOException("Mkdirs failed to create " + parent.toString());
    }
    
    AppEngineFile file = fileService.createNewBlobFile("application/octet-stream");
    FileDescriptor desc = newFile(path, file);
    desc.persist();
    
    
    FileSystem.Statistics stats = new Statistics(SCHEME);
    return new FSDataOutputStream(new AppEngineOutputStream(fileService.openWriteChannel(file, true)), stats);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
   
    AppEngineFile file = fileForPath(f);
    FileSystem.Statistics stats = new Statistics(SCHEME);
    return new FSDataOutputStream(Channels.newOutputStream(fileService.openWriteChannel(file, true)), stats);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    
    Transaction tx = datastore.beginTransaction();
    FileDescriptor srcDesc;
    try {
      srcDesc = new FileDescriptor( datastore.get(tx, keyForPath(src)) );
    } catch (EntityNotFoundException e) {
      tx.rollback();
      throw new FileNotFoundException(src.toString());
    }
    if(srcDesc.isDirectory()) {
      tx.rollback();
      throw new IOException("renaming directories is not supported");
    }

    try {
      FileDescriptor dstDesc = newFile(dst, srcDesc.getAppEngineFile());
      
      try { 
        datastore.get(tx, srcDesc.getKey());
        tx.rollback();
        throw new IOException("destination file " + dst.toString() + " already exists");
      } catch(EntityNotFoundException e) {
        // we're ok, the dest file doesn't exist.
      }
            
      datastore.put(tx, dstDesc.getEntity());
      datastore.delete(tx, srcDesc.getKey());
      tx.commit();
    } catch(Exception e) {
      tx.rollback();
      throw new IOException("Exception thrown while renaming file", e);
    }
    return true;
  }

  @Override
  public boolean delete(Path f) throws IOException {
    FileDescriptor desc = descForPath(f);
    desc.delete();
    return true;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    FileDescriptor desc = descForPath(f);
    if(recursive && desc.isDirectory()) {
      throw new UnsupportedOperationException("not implemented!");
    }
    desc.delete();
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    f = makeAbsolute(f);
    
    List<FileStatus> list = new ArrayList<FileStatus>();
    
    Query query = new Query(FILE_DESC_KIND)
      .addFilter("parent", FilterOperator.EQUAL, f.toString());
    
    for(Entity entity : datastore.prepare(query).asIterable()) {
      list.add(new FileDescriptor(entity).getFileStatus());
    }
    
    return list.toArray(new FileStatus[list.size()]);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    this.workingDirectory = new_dir;
  }

  @Override
  public Path getWorkingDirectory() {
    return this.workingDirectory;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    f = makeAbsolute(f);
    if(f.getParent() == null) {
      return true;
    }
    try {
      FileDescriptor dir = descForPath(f);
      if(!dir.isDirectory()) {
        throw new IOException(f.toString() + " is not a directory");
      }
    } catch(FileNotFoundException e) {
      mkdirs(f.getParent(), permission);
      FileDescriptor newDesc = newDirectory(f);
      newDesc.persist();
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return descForPath(f).getFileStatus();
  }
 
  private Key keyForPath(Path f) {
    return KeyFactory.createKey(FILE_DESC_KIND, f.toString());
  }

 
  private FileDescriptor newFile(Path path, AppEngineFile file) {
    FileDescriptor desc = new FileDescriptor();
    desc.entity = new Entity(keyForPath(path));
    desc.entity.setProperty("parent", path.getParent().toString());
    desc.entity.setUnindexedProperty("fullPath", file.getFullPath());
    desc.entity.setUnindexedProperty("creationTime", new Date().getTime());
    
    return desc;
  }
  
  private FileDescriptor newDirectory(Path path) {
    FileDescriptor desc = new FileDescriptor();
    desc.entity = new Entity(keyForPath(path));
    desc.entity.setProperty("parent", path.getParent().toString());
    desc.entity.setUnindexedProperty("directory", true); 
    desc.entity.setUnindexedProperty("creationTime", new Date().getTime());
    
    return desc;
  }
  
  
  public class FileDescriptor  {
    
    private Entity entity;
    
    private FileDescriptor() {
      
    }
   
    public FileDescriptor(Entity entity) {
      this.entity = entity;
    }
    
    public boolean isDirectory() {
      return entity.getProperty("directory") != null;
    }
    
    public Entity getEntity() {
      return entity;
    }
    
    public Key getKey() {
      return entity.getKey();
    }
    
    public AppEngineFile getAppEngineFile() {
      return new AppEngineFile( (String) entity.getProperty("fullPath") );
    }
    
    public BlobKey getBlobKey() throws IOException {
      AppEngineFile file = getAppEngineFile();
      BlobKey key = fileService.getBlobKey( file );
      if(key == null) {
        finalize();
        key = fileService.getBlobKey( file );
        if(key == null) {
          throw new IOException("Could not get blobKey for AppEngine file, '" + file.getFullPath() +
              ", even after finalizing.");
        }
      }
      return key;
    }
    
    public void finalize() throws IOException {
      try {
        FileWriteChannel channel = fileService.openWriteChannel(getAppEngineFile(), true);
        channel.closeFinally();
      } catch (FinalizationException e) {
        // ok
        return;
      }
    }
    
    public BlobInfo getBlobInfo() throws IOException {
      return blobInfoFactory.loadBlobInfo(getBlobKey());
    }
    
    public long getCreationTime() {
      return (Long)entity.getProperty("creationTime");
    }
    
    public Path getPath() {
      return new Path(getKey().getName());
    }
    
    public FileStatus getFileStatus() throws IOException {
      if(isDirectory()) {
        return new FileStatus(0, true, BLOCK_REPLICATION, BLOCK_SIZE, getCreationTime(), getPath());
      } else {
        return new FileStatus(getBlobInfo().getSize(), false, 1, 1024, getCreationTime(), getPath());
      }
    }
    
    public void persist() {
      datastore.put(entity);
    }
    
    public void delete() throws IOException {
      blobstore.delete(getBlobKey());
      datastore.delete(getKey());
    }
  }

}
