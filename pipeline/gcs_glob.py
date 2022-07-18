"""Utility to find GCS files matching a glob pattern.

This code is copied and adapted from apache_beam.io.gcp.gcsio.GcsIO.glob,
which is not included in my distribution of beam for reasons I could not 
determine. That code comes with the following license:

    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at
    
       http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

See original source at 
https://beam.apache.org/releases/pydoc/2.2.0/_modules/apache_beam/io/gcp/gcsio.html
"""

import fnmatch
import re

from google.cloud import storage


def parse_gcs_path(gcs_path):
    """Return the bucket and object names of the given gs:// path, or None
    if the given path is not a valid gs:// path.
    """
    match = re.match('^gs://([^/]+)/(.+)$', gcs_path)
    if match is None:
        return None
    return match.group(1), match.group(2)


def glob(pattern):
    """Return the GCS path names matching a given path name pattern.

    Path name patterns are those recognized by fnmatch.fnmatch().  The path
    can contain glob characters (*, ?, and [...] sets).

    Args:
      pattern: GCS file path pattern in the form gs://<bucket>/<name_pattern>.
      limit: Maximal number of path names to return.
        All matching paths are returned if set to None.

    Returns:
      list of GCS file paths matching the given pattern.
    """
    client = storage.Client()

    bucket, name_pattern = parse_gcs_path(pattern)
    # Get the prefix with which we can list objects in the given bucket.
    prefix = re.match('^[^[*?]*', name_pattern).group(0)

    object_paths = []
    for blob in client.list_blobs(bucket, prefix=prefix):
        if fnmatch.fnmatch(blob.name, name_pattern):
            object_paths.append('gs://%s/%s' % (bucket, blob.name))
    return object_paths

