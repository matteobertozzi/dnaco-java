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

import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import namedtuple
from functools import cmp_to_key
from urllib import request

MVN_REPO_URL = 'https://repo1.maven.org/maven2/'
TEMPLATE_VAR_PATTERN = re.compile(r'\$\{(.*?)}')
RE_URL = re.compile(r'href=[\'"]?([^\'" >]+)')
RE_VERSION = re.compile(r'[0-9]+\.[0-9]+\.*[0-9]*[0-9a-zA-Z+\.-]*')

Dependency = namedtuple('Dependency', ['group_id', 'artifact_id', 'version_variable', 'version'])

# ====================================================================== POM PARSE UTIL =====
MISSING_GROUP_IDS = {
  'maven-surefire-plugin': 'org.apache.maven.plugins',
  'maven-compiler-plugin': 'org.apache.maven.plugins',
  'maven-failsafe-plugin': 'org.apache.maven.plugins',
}

def _xml_extract_text(node, defaultValue=None):
  return node.text if node is not None else defaultValue

def _pom_extract_dependency(node, ns, properties):
  artifact_id = node.find('mvn:artifactId', ns).text
  group_id = _xml_extract_text(node.find('mvn:groupId', ns), MISSING_GROUP_IDS.get(artifact_id))
  if group_id is None: print('NULL GROUP ID', artifact_id)
  version = _xml_extract_text(node.find('mvn:version', ns))
  match = TEMPLATE_VAR_PATTERN.match(version) if version else None
  if match:
    version_variable = match.group(1)
    version = properties[match.group(1)]
  else:
    version_variable = None
  return Dependency(group_id, artifact_id, version_variable, version)

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

  for dependencies in root.findall('mvn:dependencies', ns):
    for dependency in dependencies:
      yield _pom_extract_dependency(dependency, ns, properties)

  for build in root.findall('mvn:build', ns):
    for plugins in build.findall('mvn:plugins', ns):
      for plugin in plugins:
        yield _pom_extract_dependency(plugin, ns, properties)

# ====================================================================== MAVEN REPO UTIL =====
def split_version(v):
  items = []
  for p in re.split('\\.|\\-', v):
    try:
      items.append(int(p))
    except ValueError:
      items.append(p)
  return items

def compare_version(a, b):
  av = split_version(a)
  bv = split_version(b)
  try:
    for ap, bp in zip(av, bv):
      if ap < bp:
        return -1
      elif ap > bp:
        return 1
    return len(av) - len(bv)
  except:
    return len(av) - len(bv)

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
        versions[k] = sorted(versions[k], reverse=True, key=cmp_to_key(compare_version))
      return versions
  except Exception as e:
    print('fail', pkg, MVN_REPO_URL + pkg, e)
    return None

def find_next_major(versions, major):
  major = int(major)
  for m in versions:
    if '.' not in m and (int(m) > major):
      yield versions[m][0]

# ====================================================================== XXX =====
def _scan_for_pom_files(path):
  for root, dirs, files in os.walk(path):
    for filename in files:
      if filename == 'pom.xml':
        yield os.path.join(root, filename)

def _extract_pom_files_path(pomfiles):
  for path in pomfiles:
    if os.path.isdir(path):
      for pomfile in _scan_for_pom_files(path):
        yield pomfile
    elif path.endswith('pom.xml'):
      # path is expected to be a pom file
      yield path
    else:
      print('skipping "%s". expected a pom.xml file' % path)

def _replace_properties(pomfile, properties):
  with open(pomfile, 'r') as fd:
    content = fd.read()
  original_content = content

  for k, v in properties.items():
    regex = '(?<=<' + k +'>)(.*?)(?=</' + k +'>)'
    content = re.sub(regex, v, content)

  if content != original_content:
    print('[UPDT]', pomfile)
    with open(pomfile, 'w') as fd:
      fd.write(content)
  else:
    print('[KEEP]', pomfile)

def do_replace(pomfiles):
  maven_deps_path = os.environ.get('MAVEN_DEPS_VERSION')
  if not maven_deps_path:
    print('env MAVEN_DEPS_VERSION is not set')
    sys.exit(1)

  deps_properties = {}
  with open(maven_deps_path, 'r') as fd:
    maven_deps = json.load(fd)
    for group in maven_deps.values():
      for artifact in group.values():
        deps_properties[artifact['name']] = artifact['version']

  if not deps_properties:
    print('failed to load maven deps')
    sys.exit(1)

  for pomfile in _extract_pom_files_path(pomfiles):
    _replace_properties(pomfile, deps_properties)


def do_extract(pomfiles):
  deps = {}
  for pomfile in _extract_pom_files_path(pomfiles):
    for dependency in parse_pom(pomfile):
      group = deps.setdefault(dependency.group_id, {})
      group[dependency.artifact_id] = { 'name': dependency.version_variable, 'version': dependency.version }

  print(json.dumps(deps, indent=2, sort_keys=True))

def do_check(path):
  with open(path, 'r') as fd:
    maven_deps = json.load(fd)

  for group_id, group in maven_deps.items():
    for artifact_id, artifact in group.items():
      path = '%s/%s' % (group_id.replace('.', '/'), artifact_id)
      versions = fetch_versions(path)
      current_version = artifact['version']

      if not versions:
        print('unable to find versions for %s %s, current version %s' % (group_id, artifact_id, current_version))
        continue

      parts = current_version.split('.') if current_version else ['0', '0', '0']
      maj_min = parts[0] + '.' + parts[1]
      maj = parts[0]

      latest_maj_min = versions.get(maj_min, [maj_min])[0]
      latest_maj = versions[maj][0] if versions.get(maj) else '0'
      latest_next = list(find_next_major(versions, maj))
      if latest_maj_min != current_version or latest_maj != current_version or latest_next:
        print('%s: using %s' % (artifact_id, current_version))
        if latest_maj_min != current_version:
          print(' -> next %s.x: %s' % (maj_min, latest_maj_min), versions.get(maj_min))
        if latest_maj != current_version:
          print(' -> next %s.x: %s' % (maj, latest_maj))
        if latest_next:
          print(' -> next: %s' % latest_next)


if __name__ == '__main__':
  if len(sys.argv) < 2:
    print('Usage: %s' % sys.argv[0])
    print('  EXTRACT [pomfiles...]')
    print('  CHECK <deps json file>')
    print('  REPLACE <sources dir>')
    sys.exit(1)

  if sys.argv[1] == 'EXTRACT':
    do_extract(sys.argv[2:])
  elif sys.argv[1] == 'CHECK':
    do_check(sys.argv[2])
  elif sys.argv[1] == 'REPLACE':
    do_replace(sys.argv[2:])
  else:
    print('Invalid command: %s' % sys.argv[1])
    sys.exit(1)

