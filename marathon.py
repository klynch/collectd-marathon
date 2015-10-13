#! /usr/bin/python
# Copyright 2015 Kevin Lynch
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collectd
import json
import urllib2

PREFIX = "marathon"
MARATHON_HOST = "localhost"
MARATHON_PORT = 8080
MARATHON_URL = ""
VERBOSE_LOGGING = False

CONFIGS = []


def configure_callback(conf):
    """Received configuration information"""
    host = MARATHON_HOST
    port = MARATHON_PORT
    verbose_logging = VERBOSE_LOGGING

    for node in conf.children:
        if node.key == 'Host':
            host = node.values[0]
        elif node.key == 'Port':
            port = int(node.values[0])
        elif node.key == 'Verbose':
            verbose_logging = bool(node.values[0])
        else:
            collectd.warning('marathon plugin: Unknown config key: %s.' % node.key)

    CONFIGS.append({
        'host': host,
        'port': port,
        'verbose_logging': verbose_logging,
        'metrics_url': 'http://' + host + ':' + str(port) + '/metrics'
    })

    log_verbose('Configured marathon host with host=%s, port=%s' % (host, port))


def read_callback():
    """Parse stats response from Marathon"""
    log_verbose('Read callback called')
    for config in CONFIGS:
        try:
            metrics = json.load(urllib2.urlopen(config['metrics_url'], timeout=10))

            for group in ['gauges', 'histograms', 'meters', 'timers', 'counters']:
                for name, values in metrics.get(group, {}).items():
                    for metric, value in values.items():
                        if not isinstance(value, basestring):
                            dispatch_stat('gauge', '.'.join((name, metric)), value, config['verbose_logging'])
        except urllib2.URLError as e:
            collectd.error('marathon plugin: Error connecting to %s - %r' % (config['metrics_url'], e))


def dispatch_stat(type, name, value, verbose_logging):
    """Read a key from info response data and dispatch a value"""
    if value is None:
        collectd.warning('marathon plugin: Value not found for %s' % name)
        return

    log_verbose('Sending value[%s]: %s=%s' % (type, name, value), verbose_logging)

    val = collectd.Values(plugin='marathon')
    val.type = type
    val.type_instance = name
    val.values = [value]
    # https://github.com/collectd/collectd/issues/716
    val.meta = {'0': True}
    val.dispatch()


def log_verbose(msg, verbose_logging):
    if not verbose_logging:
        return
    collectd.info('marathon plugin [verbose]: %s' % msg)

collectd.register_config(configure_callback)
collectd.register_read(read_callback)
