#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import namedtuple
from urllib import request

import xml.etree.ElementTree as ET
import re
import sys

MVN_REPO_URL = 'https://repo1.maven.org/maven2/'
TEMPLATE_VAR_PATTERN = re.compile(r'\$\{(.*?)}')
RE_URL = re.compile(r'href=[\'"]?([^\'" >]+)')
RE_VERSION = re.compile(r'[0-9]+\.[0-9]+\.*[0-9]*[0-9a-zA-Z+\.-]*')
Dependency = namedtuple('Dependency', ['group_id', 'artifact_id', 'version'])

def parse_pom(path):
  ns = { 'mvn': 'http://maven.apache.org/POM/4.0.0' }
  tree = ET.parse(path)
  root = tree.getroot()

  properties = {}
  properties['project.version'] = root.find('mvn:version', ns).text
  for properties_node in root.findall('mvn:properties', ns):
    for property in properties_node:
      name = property.tag.split('}')[1]
      version = property.text
      properties[name] = version


  deps = []
  for dependencies in root.findall('mvn:dependencies', ns):
    for dependency in dependencies:
      artifact_id = dependency.find('mvn:artifactId', ns).text
      group_id = dependency.find('mvn:groupId', ns).text
      version = dependency.find('mvn:version', ns).text
      match = TEMPLATE_VAR_PATTERN.match(version)
      if match: version = properties[match.group(1)]
      deps.append(Dependency(group_id, artifact_id, version))
  return deps

def parse_version(vstr):
  version = 0
  index = 0
  for v in vstr.split('.'):
    try:
      version |= int(v) << (64 - (index * 20))
      index += 1
    except ValueError:
      try:
        for v in v.split('-'):
          version |= int(v) << (64 - (index * 20))
          index += 1
      except ValueError as e:
        pass
  return version

def fetch_versions(pkg):
  #print('fetch', MVN_REPO_URL + pkg)
  try:
    with request.urlopen(MVN_REPO_URL + pkg) as response:
      html = response.read().decode('utf-8')

      versions = {}
      for subUrl in RE_URL.findall(html):
        v = RE_VERSION.findall(subUrl)
        if not v: continue

        version = v[0]
        parts = version.split('.')
        versions.setdefault(parts[0], []).append(version)
        versions.setdefault(parts[0] + '.' + parts[1], []).append(version)

      for k in versions:
        versions[k] = sorted(versions[k], reverse=True, key=parse_version)
      return versions
  except Exception as e:
    print('fail', pkg, MVN_REPO_URL + pkg, e)
    return None

def find_next_major(versions, major):
  major = int(major)
  for m in versions:
    if '.' not in m and (int(m) > major):
      yield versions[m][0]


def dump_deps(pom_path):
  for dependency in parse_pom(pom_path):
    path = '%s/%s' % (dependency.group_id.replace('.', '/'), dependency.artifact_id)
    versions = fetch_versions(path)
    if not versions: continue

    parts = dependency.version.split('.')
    maj_min = parts[0] + '.' + parts[1]
    maj = parts[0]

    latest_maj_min = versions.get(maj_min, [maj_min])[0]
    if maj in versions:
      latest_maj = versions[maj][0]
      latest_next = list(find_next_major(versions, maj))
    else:
      latest_maj = 0
      latest_next = 0
    if latest_maj_min != dependency.version or latest_maj != dependency.version or latest_next:
      print('%s: using %s' % (dependency.artifact_id, dependency.version))
      if latest_maj_min != dependency.version:
        print(' -> next %s.x: %s' % (maj_min, latest_maj_min), versions[maj_min])
      if latest_maj != dependency.version:
        print(' -> next %s.x: %s' % (maj, latest_maj))
      if latest_next:
        print(' -> next: %s' % latest_next)

if __name__ == '__main__':
  if len(sys.argv) < 2:
    print('Usage: maven-deps <pom path> [<pom path>, ...]')
    sys.exit(1)

  for pom_path in sys.argv[1:]:
    print(pom_path)
    dump_deps(pom_path)
