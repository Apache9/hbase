package org.apache.hadoop.hbase.util;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CommonFSUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CommonFSUtils.class);

  // this mapping means that under a federated FileSystem implementation, we'll
  // only log the first failure from any of the underlying FileSystems at WARN and all others
  // will be at DEBUG.
  private static final Map<FileSystem, Boolean> warningMap = new ConcurrentHashMap<>();

  /**
   * Sets storage policy for given path.
   * If the passed path is a directory, we'll set the storage policy for all files
   * created in the future in said directory. Note that this change in storage
   * policy takes place at the FileSystem level; it will persist beyond this RS's lifecycle.
   * If we're running on a version of FileSystem that doesn't support the given storage policy
   * (or storage policies at all), then we'll issue a log message and continue.
   *
   * See http://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html
   *
   * @param fs We only do anything it implements a setStoragePolicy method
   * @param path the Path whose storage policy is to be set
   * @param storagePolicy Policy to set on <code>path</code>; see hadoop 2.6+
   * org.apache.hadoop.hdfs.protocol.HdfsConstants for possible list e.g
   * 'COLD', 'WARM', 'HOT', 'ONE_SSD', 'ALL_SSD', 'LAZY_PERSIST'.
   */
  public static void setStoragePolicy(final FileSystem fs, final Path path,
      final String storagePolicy) {
    try {
      setStoragePolicy(fs, path, storagePolicy, false);
    } catch (IOException e) {
      // should never arrive here
      LOG.warn("We have chosen not to throw exception but some unexpectedly thrown out", e);
    }
  }

  static void setStoragePolicy(final FileSystem fs, final Path path, final String storagePolicy,
      boolean throwException) throws IOException {
    if (storagePolicy == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("We were passed a null storagePolicy, exiting early.");
      }
      return;
    }
    String trimmedStoragePolicy = storagePolicy.trim();
    if (trimmedStoragePolicy.isEmpty()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("We were passed an empty storagePolicy, exiting early.");
      }
      return;
    } else {
      trimmedStoragePolicy = trimmedStoragePolicy.toUpperCase(Locale.ROOT);
    }
    if (trimmedStoragePolicy.equals(HConstants.DEFER_TO_HDFS_STORAGE_POLICY)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("We were passed the defer-to-hdfs policy {}, exiting early.",
            trimmedStoragePolicy);
      }
      return;
    }
    try {
      invokeSetStoragePolicy(fs, path, trimmedStoragePolicy);
    } catch (IOException e) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Failed to invoke set storage policy API on FS", e);
      }
      if (throwException) {
        throw e;
      }
    }
  }

  /*
   * All args have been checked and are good. Run the setStoragePolicy invocation.
   */
  private static void invokeSetStoragePolicy(final FileSystem fs, final Path path,
      final String storagePolicy) throws IOException {
    Method m = null;
    Exception toThrow = null;
    try {
      m = fs.getClass().getDeclaredMethod("setStoragePolicy", Path.class, String.class);
      m.setAccessible(true);
    } catch (NoSuchMethodException e) {
      toThrow = e;
      final String msg = "FileSystem doesn't support setStoragePolicy; HDFS-6584, HDFS-9345 "
          + "not available. This is normal and expected on earlier Hadoop versions.";
      if (!warningMap.containsKey(fs)) {
        warningMap.put(fs, true);
        LOG.warn(msg, e);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(msg, e);
      }
      m = null;
    } catch (SecurityException e) {
      toThrow = e;
      final String msg = "No access to setStoragePolicy on FileSystem from the SecurityManager; "
          + "HDFS-6584, HDFS-9345 not available. This is unusual and probably warrants an email "
          + "to the user@hbase mailing list. Please be sure to include a link to your configs, and "
          + "logs that include this message and period of time before it. Logs around service "
          + "start up will probably be useful as well.";
      if (!warningMap.containsKey(fs)) {
        warningMap.put(fs, true);
        LOG.warn(msg, e);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(msg, e);
      }
      m = null; // could happen on setAccessible() or getDeclaredMethod()
    }
    if (m != null) {
      try {
        m.invoke(fs, path, storagePolicy);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Set storagePolicy={} for path={}", storagePolicy, path);
        }
      } catch (Exception e) {
        toThrow = e;
        // This swallows FNFE, should we be throwing it? seems more likely to indicate dev
        // misuse than a runtime problem with HDFS.
        if (!warningMap.containsKey(fs)) {
          warningMap.put(fs, true);
          LOG.warn("Unable to set storagePolicy=" + storagePolicy + " for path=" + path + ". "
              + "DEBUG log level might have more details.", e);
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("Unable to set storagePolicy=" + storagePolicy + " for path=" + path, e);
        }
        // check for lack of HDFS-7228
        if (e instanceof InvocationTargetException) {
          final Throwable exception = e.getCause();
          if (exception instanceof RemoteException && HadoopIllegalArgumentException.class.getName()
              .equals(((RemoteException) exception).getClassName())) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Given storage policy, '" + storagePolicy + "', was rejected and probably "
                  + "isn't a valid policy for the version of Hadoop you're running. I.e. if you're "
                  + "trying to use SSD related policies then you're likely missing HDFS-7228. For "
                  + "more information see the 'ArchivalStorage' docs for your Hadoop release.");
            }
            // Hadoop 2.8+, 3.0-a1+ added FileSystem.setStoragePolicy with a default implementation
            // that throws UnsupportedOperationException
          } else if (exception instanceof UnsupportedOperationException) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("The underlying FileSystem implementation doesn't support "
                  + "setStoragePolicy. This is probably intentional on their part, since HDFS-9345 "
                  + "appears to be present in your version of Hadoop. For more information check "
                  + "the Hadoop documentation on 'ArchivalStorage', the Hadoop FileSystem "
                  + "specification docs from HADOOP-11981, and/or related documentation from the "
                  + "provider of the underlying FileSystem (its name should appear in the "
                  + "stacktrace that accompanies this message). Note in particular that Hadoop's "
                  + "local filesystem implementation doesn't support storage policies.", exception);
            }
          }
        }
      }
    }
    if (toThrow != null) {
      throw new IOException(toThrow);
    }
  }
}
