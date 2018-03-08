#! /usr/bin/python

"""
    InfluxDB data exploration and checking
    Copyright (C) 2018 Francesco Melchiori
    <https://www.francescomelchiori.com/>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import os
import json
import urllib
import urllib2


class CustomerData:
    def __init__(self, customer_name, json_path=''):
        self.customer_name = customer_name
        self.json_path = json_path
        self.check_map = {}
        self.load_check_map_json()

    def __repr__(self):
        print_message = "Customer name: '{0}'\n".format(self.customer_name)
        print_message += "JSON path: "
        if not self.json_path.find('\\') >= 0:
            print_message += "'{0}\\'".format(os.getcwd())
        print_message += "'{0}'\n".format(self.json_path)
        print_message += 'Check map: {0}\n'.format(self.check_map)
        return print_message

    def load_check_map_json(self):
        if not self.json_path:
            self.json_path = '{0}_check_map.json'.format(self.customer_name)
        check_map = load_json(self.json_path)
        for customer_data in check_map['customers']:
            if customer_data['customer_name'] == self.customer_name:
                self.check_map = customer_data
        if not self.check_map:
            raise DataNotFound(data_name=self.customer_name,
                               source_name='customers')
        return True


class CustomerInfluxDBData(CustomerData):
    def __init__(self, customer_name, json_path=''):
        CustomerData.__init__(self, customer_name, json_path)
        self.data_source_name = ''
        self.data_source_ip_port = ''
        self.databases = []
        self.load_influxdb_check_map()

    def __repr__(self):
        print_message = "Data source name: '{0}'\n".format(
            self.data_source_name)
        print_message += "Data source ip port: '{0}'\n".format(
            self.data_source_ip_port)
        print_message += 'Databases: {0}\n'.format(self.databases)
        return print_message

    def load_influxdb_check_map(self):
        for data_source in self.check_map['data_sources']:
            if data_source['data_source_name'] == 'influxdb':
                self.data_source_name = data_source['data_source_name']
                self.data_source_ip_port = data_source['data_source_ip_port']
                self.databases = data_source['databases']
        if not self.data_source_name:
            raise DataNotFound(data_name='influxdb',
                               source_name='data_sources')
        return True


class DataNotFound(Exception):
    def __init__(self, data_name='', source_name=''):
        self.data_name = data_name
        self.source_name = source_name

    def __str__(self):
        exception_message = ''
        if self.data_name:
            exception_message += "'{0}'".format(self.data_name)
        if self.source_name:
            exception_message += " from '{0}'".format(self.source_name)
        return exception_message


def load_json(file_path):
    try:
        json_file = open(file_path)
    except IOError:
        print('error | json file opening issue')
        return False
    try:
        json_object = json.load(json_file)
    except ValueError:
        print('error | json file loading issue')
        return False
    return json_object


def get_influxdb_data(ip, database, measure, minutes, port='8086',
                      features=None, feature_filter=None, feature_order='asc'):
    influxdb_base_url = 'http://{0}:{1}/query'.format(ip, port)
    if features is None:
        features = ['*']
    influxdb_query_features = 'SELECT ' + ', '.join(features)
    influxdb_query_measure = 'FROM {0}'.format(measure)
    influxdb_query_selection = 'WHERE time > now() - {0}m ' \
                               'AND time < now()'.format(minutes)
    if feature_filter:
        feature_filter_influxdb_format = ["{0} = '{1}'".format(
            feature_name, feature_value)
            for feature_name, feature_value
            in feature_filter.items()]
        influxdb_query_selection += 'AND ' + ' AND '.join(
            feature_filter_influxdb_format)
    influxdb_query_order = 'ORDER BY time ' + \
                           'ASC' if feature_order == 'asc' else 'DESC'
    influxdb_query = ' '.join([influxdb_query_features, influxdb_query_measure,
                               influxdb_query_selection, influxdb_query_order])
    influxdb_query_url = urllib.urlencode({'q': influxdb_query,
                                           'db': database,
                                           'pretty': 'true'})
    influxdb_request = '{0}?{1}'.format(influxdb_base_url, influxdb_query_url)
    influxdb_response = urllib2.urlopen(influxdb_request)
    # print(influxdb_response.geturl())
    # print(influxdb_response.info())
    # print(influxdb_response.getcode())
    # print(influxdb_response.read())
    return influxdb_response


if __name__ == '__main__':
    # print(CustomerData('bossard'))
    # print(CustomerInfluxDBData('bossard'))
    get_influxdb_data('10.62.5.117', 'bossard', 'alyvix', 1)
