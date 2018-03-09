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


class CustomerInfluxDBCheck(CustomerInfluxDBData):
    def __init__(self, customer_name, json_path=''):
        CustomerInfluxDBData.__init__(self, customer_name, json_path)
        self.data_source_ip = self.data_source_ip_port.split(':')[0]
        self.data_source_port = self.data_source_ip_port.split(':')[1]
        self.check_sequence = [['check_name', [
            'database', 'measurement', 'host', 'test_name',
            'transaction_name']]]
        self.get_check_sequence()
        self.run_check_sequence()

    def __repr__(self):
        print_message = 'Check tree:\n'
        for check in self.check_sequence:
            print_message += '{0}\n'.format(check)
        return print_message

    def get_check_sequence(self):
        for database in self.databases:
            database_name = database['database']
            for measurement in database['measurements']:
                measurement_name = measurement['measurement']
                for host in measurement['hosts']:
                    host_name = host['host']
                    for test in host['tests']:
                        test_name = test['test_name']
                        for transaction in test['transactions']:
                            transaction_name = transaction['transaction_name']
                            for check in transaction['checks']:
                                check_name = check['check_name']
                                check_feature = [
                                    self.data_source_ip,
                                    self.data_source_port,
                                    database_name,
                                    measurement_name,
                                    host_name,
                                    test_name,
                                    transaction_name]
                                if check_name == 'check_feature_availability':
                                    check_feature.append(check['feature_name'])
                                    check_feature.append(check['measure_unit'])
                                    check_feature.append(check['sanity_period'])
                                self.check_sequence.append([check_name,
                                                            check_feature])

    def run_check_sequence(self):
        for check in self.check_sequence:
            check_name = check[0]
            check_args = check[1]
            if check_name == 'check_feature_availability':
                check_feature_availability(*check_args)


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


def get_influxdb_data(ip, database, measure, seconds_from_now, port='8086',
                      features=None, feature_filter=None, feature_order='desc'):
    """
        database: '<influxdb_database_name>'
        measure: '<influxdb_measurement_name>'
        features: [<list_of_features_to_fetch>]
        feature_filter: {<dict_of_features_and_their_values_to_filter_in>}
        feature_order: 'asc' or 'desc'
    """
    influxdb_base_url = 'http://{0}:{1}/query'.format(ip, port)
    if features is None:
        features = ['*']
    influxdb_query_features = 'SELECT ' + ', '.join(features)
    influxdb_query_measure = 'FROM {0}'.format(measure)
    influxdb_query_selection = 'WHERE time > now() - {0}s ' \
                               'AND time < now()'.format(seconds_from_now)
    if feature_filter:
        feature_filter_influxdb_format = ["{0} = '{1}'".format(
            feature_name, feature_value)
            for feature_name, feature_value
            in feature_filter.items()]
        influxdb_query_selection += ' AND ' + ' AND '.join(
            feature_filter_influxdb_format)
    influxdb_query_order = 'ORDER BY time '
    if feature_order == 'asc':
        influxdb_query_order += 'ASC'
    elif feature_order == 'desc':
        influxdb_query_order += 'DESC'
    influxdb_query = ' '.join([influxdb_query_features, influxdb_query_measure,
                               influxdb_query_selection, influxdb_query_order])
    influxdb_query_url = urllib.urlencode({'q': influxdb_query,
                                           'db': database})
    influxdb_request = '{0}?{1}'.format(influxdb_base_url, influxdb_query_url)
    influxdb_response = json.load(urllib2.urlopen(influxdb_request))
    # print(influxdb_response)
    # print_influxdb_data(influxdb_response)
    return influxdb_response


def print_influxdb_data(influxdb_data):
    if 'series' in influxdb_data['results'][0].keys():
        print(influxdb_data['results'][0]['series'][0]['columns'])
        values = influxdb_data['results'][0]['series'][0]['values']
        for value in values:
            print(value)
        return True
    else:
        print('no results')
        return False


def check_feature_availability(ip, port, database, measure, host, testcase,
                               transaction, feature_name, measure_unit,
                               sanity_period):
    feature_filter = {'host': host, 'test_name': testcase,
                      'transaction_name': transaction}
    seconds_converter = {'seconds': 1, 'minutes': 60, 'hours': 60*60,
                         'days': 60*60*24}
    seconds_from_now = sanity_period * seconds_converter[measure_unit]
    features = ['time', 'host', 'test_name', 'transaction_name', 'performance',
                'warning_threshold', 'critical_threshold', 'state']
    influxdb_response = get_influxdb_data(ip=ip, port=port,
                                          database=database,
                                          measure=measure,
                                          seconds_from_now=seconds_from_now,
                                          features=features,
                                          feature_filter=feature_filter,
                                          feature_order='desc')
    # print(influxdb_response)
    # print_influxdb_data(influxdb_response)
    if 'series' in influxdb_response['results'][0].keys():
        influxdb_response_features = influxdb_response[
            'results'][0]['series'][0]['columns']
        # print(influxdb_response_features)
    else:
        print('no results')
        return False
    if feature_name in influxdb_response_features:
        timestamp_index = influxdb_response_features.index('time')
        feature_index = influxdb_response_features.index(feature_name)
        measure_points = influxdb_response[
            'results'][0]['series'][0]['values']
        check_sequence = []
        for measure_point in measure_points:
            # print(measure_point)
            timestamp = measure_point[timestamp_index]
            measure_state = measure_point[feature_index]
            measure_check = 1 if measure_state == 'ok' else 0
            check_sequence.append((measure_check, timestamp))
        # print(availability_sequence)
        check = check_availability_sequence(check_sequence,
                                            'at_least_one_ok')
        print(check)
        return check
    else:
        print('no feature')
        return False


def check_availability_sequence(availability_sequence, availability_mode):
    if availability_mode == 'at_least_one_ok':
        # print(availability_sequence)
        for (measure_check, timestamp) in availability_sequence:
            if measure_check == 1:
                return 1
        return 0


if __name__ == '__main__':
    # print(CustomerData('customer'))
    # print(CustomerInfluxDBData('customer'))
    # print(CustomerInfluxDBCheck('customer'))
    CustomerInfluxDBCheck('customer')

    # get_influxdb_data(ip='<ip>',
    #                   database='<customer_name>',
    #                   measure='<measurement>',
    #                   seconds_from_now=<sanity_seconds>)
    # check_feature_availability(ip='<ip>',
    #                            port='<port>',
    #                            database='<customer_name>',
    #                            measure='<measurement>',
    #                            host='<host>',
    #                            testcase='<test_name>',
    #                            transaction='<transaction_name>',
    #                            feature_name='<feature_name>',
    #                            measure_unit='<measure_unit>',
    #                            sanity_period=<sanity_period>)
