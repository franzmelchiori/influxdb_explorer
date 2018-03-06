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

import json


class CustomerData:
    def __init__(self, customer_name, json_path=''):
        self.customer_name = customer_name
        self.json_path = json_path
        self.check_map = {}
        self.load_check_map_json()

    def __repr__(self):
        return '{0}'.format(self.check_map)

    def load_check_map_json(self):
        if self.json_path:
            file_path = self.json_path
        else:
            file_path = '{0}_check_map.json'.format(self.customer_name)
        check_map = load_json(file_path)
        for customer_data in check_map['customers']:
            if customer_data['customer_name'] == self.customer_name:
                self.check_map = customer_data
        if not self.check_map:
            raise DataNotFound(data_name=self.customer_name,
                               source_name='customers')
        return True


class CustomerInfluxDBData(CustomerData):
    def __init__(self, customer_name, json_path=''):
        self.customer_name = customer_name
        self.json_path = json_path
        self.data_source_name = ''
        self.data_source_ip_port = ''
        self.databases = []
        self.load_influxdb_check_map()

    def __repr__(self):
        return '{0}'.format(self.check_map)

    def load_influxdb_check_map(self):
        customerdata = CustomerData(self.customer_name, self.json_path)
        for data_source in customerdata.check_map['data_sources']:
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


if __name__ == '__main__':
    dc = CustomerInfluxDBData('bossard')
