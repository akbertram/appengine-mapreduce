#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#




"""Mocks for classes defined in webapp module.

Use this classes to test functionality depending on webapp framework.
"""


import StringIO
import urlparse

class MockRequest(object):
  """Mocks out webapp.Request.

  Use get()/set() to configure the query parameters for the request.

  Public Members:
    method: A string representing the request type.  Defaults to 'GET'.
    uri: A string representing the requested URI.  Defaults to '/start'.
  """

  uri = property(lambda self: self.url)

  def __init__(self):
    """Initializer."""
    self.method = 'GET'
    self.scheme = 'http'
    self.host = 'foo.com'
    self._path = '/start'
    self.params = {}
    self.params_list = []
    self.headers = {}
    self.body = ''
    self.url = ''
    self.path_qs = ''
    self.update_properties()

  def get_path(self):
    return self._path

  def set_path(self, value):
    self._path = value
    self.update_properties()

  path = property(get_path, set_path)

  def get(self, argument_name, default_value='', allow_multiple=False):
    """Looks up the value of a query parameter.

    Args:
      argument_name: The query parameter key as a string.
      default_value: The default query parameter value as a string if it was
        not supplied.
      allow_multiple: return a list of values with the given name

    Returns:
      If allow_multiple is False (which it is by default), we return the first
      value with the given name given in the request. If it is True, we always
      return an list.
    """
    if argument_name not in self.params:
      if allow_multiple:
        return []
      return default_value

    if allow_multiple:
      return list(self.params[argument_name])


    if isinstance(self.params[argument_name], list):

      return self.params[argument_name][0]
    return self.params[argument_name]

  def get_all(self, argument_name):
    """Returns a list of query parameters with the given name.

    Args:
      argument_name: the name of the query argument.

    Returns:
      A (possibly empty) list of values.
    """
    if argument_name in self.params:
      if isinstance(self.params[argument_name], list):
        return self.params[argument_name]
      else:
        return [self.params[argument_name]]
    return []

  def set(self, argument_name, value):
    """Sets the value of a query parameter.

    Args:
      argument_name: The string name of the query parameter.
      value: The string value of the query parameter. Pass None to remove
        query parameter.
    """
    self.params_list = filter(lambda p: p[0] != argument_name, self.params_list)

    if value is not None:
      self.params[argument_name] = value
      if type(value) == list:
        for v in value:
          self.params_list.append((argument_name, v))
      else:
        self.params_list.append((argument_name, value))
    else:
      del self.params[argument_name]
    self.update_properties()

  def relative_url(self, other_url, to_application=False):
    """Return an absolute (!) URL by combining self.path with other_url."""
    url = '%s://%s/' % (self.scheme, self.host)
    return urlparse.urljoin(url, other_url)

  def update_properties(self):
    """Update url, path_qs property to be in sync with path and params."""
    self.path_qs = self._path

    params_qs = ''
    for param_value_pair in self.params_list:
      if params_qs:
        params_qs += '&'
      params_qs += param_value_pair[0] + "=" + param_value_pair[1]

    if params_qs:
      self.path_qs += '?' + params_qs
    self.url = self.scheme + '://' + self.host + self.path_qs

  def arguments(self):
    """Gets the set of argument names used in this request."""
    return list(set(p[0] for p in self.params_list))


class MockResponse(object):
  """Mocks out webapp.Response.

  Public Members:
    out: A StringIO instance.
    status: HTTP status code.
    message: HTTP status message.
  """

  def __init__(self):
    self.out = StringIO.StringIO()
    self.headers = {}
    self.status = 200
    self.status_message = 'OK'

  def set_status(self, status, message=None):
    """Sets the value of status.

    Args:
      status: HTTP status code.
      message: HTTP status message.
    """
    self.status = status
    if message:
      self.status_message = message

  def has_error(self):
    """Indicates whether the response was an error response."""
    return self.status >= 400

  def clear(self):
    """Clears all data written to self.out."""
    self.out.seek(0)
    self.out.truncate(0)
