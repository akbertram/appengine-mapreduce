package com.google.appengine.tools.mapreduce;

import javax.servlet.http.HttpServletRequest;

public class UrlUtil {

  private UrlUtil() {

  }

  /**
   * Returns the portion of the URL from the end of the TLD (exclusive) to the
   * handler portion (exclusive).
   *
   * For example, getBase(https://www.google.com/foo/bar) -> /foo/
   * However, there are handler portions that take more than segment
   * (currently only the command handlers). So in that case, we have:
   * getBase(https://www.google.com/foo/command/bar) -> /foo/
   */
  public static String getBase(HttpServletRequest request) {
    String fullPath = request.getRequestURI();
    int baseEnd = getDividingIndex(fullPath);
    return fullPath.substring(0, baseEnd + 1);
  }

  /**
   * Finds the index of the "/" separating the base from the handler.
   */
  public static int getDividingIndex(String fullPath) {
    int baseEnd = fullPath.lastIndexOf("/");
    if (fullPath.substring(0, baseEnd).endsWith(MapReduceServlet.COMMAND_PATH)) {
      baseEnd = fullPath.substring(0, baseEnd).lastIndexOf("/");
    }
    return baseEnd;
  }
}
