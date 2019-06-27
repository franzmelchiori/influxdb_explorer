#! /usr/bin/python

"""
    InfluxDB data exploration and checking
    Copyright (C) 2019 Francesco Melchiori
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
    along with this program.  If not, see
    <http://www.gnu.org/licenses/>.

    mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmddddmdmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmmmmmmmmddhddy/:/+oomdmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmmmmmmms:/.:o:---..:+++syhdmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmmmddyo:--`.-...```````-ssddmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmmmmh/-`````````````````.ssdmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmdo+..```-+osoo/`.```````--hdmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmdh:-..``-oyhhhhy+..`````` `./hmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmy:--.`.ohhhhhhhyso/..``````.-hmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmy--:.``:hdhhhyysossso/--..-.`.:dmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmh/-.-.``ohh+:-.`.-/+++-``.:+o``.dmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmm/....``/hy:.-.``-/oys/-```..o.`-mmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmm-...``/hho:/:..`-oyhhs.```.-+-`-mmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmms.````shhyyso++syhhhys+:--::o/`:mmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmms::``ohhhhhhysoos/-/::+++++o+`+mmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmd//+.:yhhyso//+s+:--..::/+oo/:dmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmd/s/y/syso+:::+/::----:::/+o-:mmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmdssysyyo//::--://---..--:/o:ymmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmds//yso/////++:-..---.://oymmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmd/`ss++/+++++/:-..---//ommmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmmmy+s+///++/:--..--://:smmmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmdy/-+s++///:--------../hmmmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmdhsss:::oo///::----..``/hhhdmmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmdhssso-:/+/+++:---..``.yhyyhmmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmdyss+:-/////:--...../hyyyydmmmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmmdy+--////::---....shyyyhhddmmmmmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmmmdhs::::::::----/syyyshyyhhhhddmmmmmmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmmdddhy:::::::---/yyyyssyyyyyyhhhdddddmmmmmmmmmmmmm
    mmmmmmmmmmmmmmmmmmdddhhyyo/-----::yysosyyyyyhhhhhhhhddddddddmmmmmmmm
    mmmmmmmmdddddddddddddddhhhhy:./oysooosssssssssyyyyyyyyyyhyyhhdddddmm
    mmmddddddddhhhhhhhhhhhhhhhhhyssooooosssyyyyyhhhhhhhhhhhddhhhhhhysssd
    mdddddddddddddddddddddddddhyyyyyyyyhhhhhhhhhhhhdddddddhhhhhhyhyssssy
    ddddddddddddddddddddddddddhhhhhhhhhhhhhhhhhyyyyyyyhhhhhhhhyyhysosssy
"""

import sys
import os
import argparse
import json
import urllib
import urllib2


error_level = {'OK': 0,
               'WARNING': 1,
               'CRITICAL': 2,
               'UNKNOWN': 3}


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
            self.json_path = 'check_map.json'
            # self.json_path = '{0}_check_map.json'.format(self.customer_name)
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
    def __init__(self, customer_name, json_path='', verbose_level=1):
        CustomerInfluxDBData.__init__(self, customer_name, json_path)
        self.verbose_level = verbose_level
        self.data_source_ip = self.data_source_ip_port.split(':')[0]
        self.data_source_port = self.data_source_ip_port.split(':')[1]
        self.check_sequence = []
        self.get_check_sequence()
        self.run_check_sequence()
        self.check_result = error_level['UNKNOWN']
        self.analyze_check_results()

    def __repr__(self):

        def get_check_indexed_value(check_to_index, check_key):
            return check_to_index[self.check_sequence[0].index(
                check_key)]

        def get_check_indexed_detail_value(check_to_index,
                                           check_detail_key=None):
            if not check_detail_key:
                return check_to_index[1][:]
            else:
                return check_to_index[1][self.check_sequence[0][1].index(
                    check_detail_key)]

        print_message = ''
        if self.verbose_level in (1, 2):
            error_label = get_error_label(self.check_result)
            first_check = self.check_sequence[1]
            database_name = get_check_indexed_detail_value(
                first_check, 'database_name')
            print_message += "{0}: {1} ".format(error_label, database_name)
            if error_label == 'OK':
                print_message += "checks are healthy. Enjoy the hindu calm."
            else:
                print_message += "checks have some troubles. Do something."
            print_message += " | "
            print_message += "'{0}'={1};1;2;; ".format(
                database_name,
                self.check_result)
        if self.verbose_level == 2:
            print_message += " | "
            for check in self.check_sequence[1:]:
                host_name = get_check_indexed_detail_value(
                    check, 'host_name')
                test_name = get_check_indexed_detail_value(
                    check, 'test_name')
                transaction_name = get_check_indexed_detail_value(
                    check, 'transaction_name')
                check_result = get_check_indexed_value(
                    check, 'check_result')
                print_message += "'{0}_{1}_{2}'={3};1;2;; ".format(
                    host_name,
                    test_name,
                    transaction_name,
                    check_result)
        if self.verbose_level >= 3:
            print_message += '\n'
            print_message += 'Check results:\n'
            for check in self.check_sequence[1:]:
                error_label = get_error_label(get_check_indexed_value(
                    check, 'check_result'))
                check_name = get_check_indexed_value(
                    check, 'check_name')
                print_message += '[ {0} '.format(error_label)
                print_message += '| {0} '.format(check_name)
                for feature in get_check_indexed_detail_value(check):
                    print_message += '| {0} '.format(feature)
                print_message += ']\n'
        return print_message

    def get_check_sequence(self):
        check_header = []
        check_headed = False
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
                                check_result = None
                                if not check_headed:
                                    check_header.append('check_name')
                                    check_header.append([
                                        'data_source_ip',
                                        'data_source_port',
                                        'database_name',
                                        'measurement_name',
                                        'host_name',
                                        'test_name',
                                        'transaction_name',
                                        'feature_name',
                                        'measure_unit',
                                        'sanity_period'])
                                    check_header.append('check_result')
                                    self.check_sequence.append(check_header)
                                    check_headed = True
                                self.check_sequence.append([check_name,
                                                            check_feature,
                                                            check_result])
        return self.check_sequence

    def run_check_sequence(self):
        for check in self.check_sequence[1:]:
            check_name = check[0]
            check_args = check[1]
            if check_name == 'check_feature_availability':
                check[2] = check_feature_availability(*check_args)
        return self.check_sequence

    def analyze_check_results(self):
        max_check_result = max([check[2] for check in self.check_sequence[1:]])
        min_check_result = min([check[2] for check in self.check_sequence[1:]])
        if max_check_result == error_level['UNKNOWN']:
            self.check_result = error_level['CRITICAL']
        elif max_check_result == error_level['CRITICAL']:
            self.check_result = error_level['CRITICAL']
        elif max_check_result == error_level['OK']:
            if min_check_result == error_level['OK']:
                self.check_result = error_level['OK']
        else:
            self.check_result = error_level['CRITICAL']

    def exit_check_result(self):
        exit(self.check_result)
        return True


class CustomersInfluxDBChecks:
    def __init__(self, json_path='', verbose_level=1):
        self.json_path = json_path
        self.verbose_level = verbose_level
        self.customer_names = []
        self.load_customer_names()
        self.customers_checks = []
        self.run_customers_checks()

    def __repr__(self):
        print_message = ''
        for customer_checks in self.customers_checks:
            print(customer_checks)
        return print_message

    def load_customer_names(self):
        if not self.json_path:
            self.json_path = 'check_map.json'
        check_map = load_json(self.json_path)
        for customer_data in check_map['customers']:
            self.customer_names.append(customer_data['customer_name'])
        if not self.customer_names:
            raise DataNotFound(data_name='customers',
                               source_name='json file')
        return True

    def run_customers_checks(self):
        for customer in self.customer_names:
            cc = CustomerInfluxDBCheck(customer_name=customer,
                                       json_path=self.json_path,
                                       verbose_level=self.verbose_level)
            self.customers_checks.append(cc)


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
        exit(error_level['UNKNOWN'])
        return False
    try:
        json_object = json.load(json_file)
    except ValueError:
        print('error | json file loading issue')
        exit(error_level['UNKNOWN'])
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
    # print(influxdb_request)
    influxdb_response = json.load(urllib2.urlopen(influxdb_request))
    # print(influxdb_response)
    # print_influxdb_data(influxdb_response)
    return influxdb_response


def print_influxdb_data(influxdb_data):
    if 'series' in influxdb_data['results'][0].keys():
        # if 'values' in influxdb_data['results'][0]['series'][0].keys():
        print(influxdb_data['results'][0]['series'][0]['columns'])
        values = influxdb_data['results'][0]['series'][0]['values']
        for value in values:
            if value:
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
        # print('no results')
        return error_level['UNKNOWN']
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
        # print(check)
        return check
    else:
        # print('no feature')
        return error_level['UNKNOWN']


def check_availability_sequence(availability_sequence, availability_mode):
    if availability_mode == 'at_least_one_ok':
        # print(availability_sequence)
        for (measure_check, timestamp) in availability_sequence:
            if measure_check == 1:
                return error_level['OK']
        return error_level['CRITICAL']


def get_error_label(error_code):
    error_level_map = error_level.items()
    for (error_label_map, error_code_map) in error_level_map:
        if error_code == error_code_map:
            error_label = error_label_map
            return error_label


def check_customer_influxdb_checks(customer, json_path='', verbose=1):
    cc = CustomerInfluxDBCheck(customer_name=customer,
                               json_path=json_path,
                               verbose_level=verbose)
    print(cc)
    cc.exit_check_result()


def check_customers_influxdb_checks(json_path='', verbose=1):
    csc = CustomersInfluxDBChecks(json_path=json_path,
                                  verbose_level=verbose)
    print(csc)


def set_check_map():
    check_map_template = """{
    "customers": [
        {
            "customer_name": "<customer_name>",
            "data_sources": [
                {
                    "data_source_name": "influxdb",
                    "data_source_ip_port": "<xxx.xxx.xxx.xxx:xxxx>",
                    "databases": [
                        {
                            "database": "<database_name>",
                            "measurements": [
                                {
                                    "measurement": "alyvix",
                                    "hosts": [
                                        {
                                            "host": "<host_name>",
                                            "tests": [
                                                {
                                                    "test_name": "<test_name>",
                                                    "transactions": [
                                                        {
                                                            "transaction_name": "<transaction_name>",
                                                            "checks": [
                                                                {
                                                                    "check_name": "check_feature_availability",
                                                                    "feature_name": "state",
                                                                    "measure_unit": "minutes",
                                                                    "sanity_period": 60
                                                                }
                                                            ]
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}"""
    print_message = '\n'
    print_message += 'CheckOfChecks\n'
    print_message += '-------------\n\n'
    file_touched = False
    if not os.path.isfile('check_map.json'):
        check_map_file = open('check_map.json', 'w')
        check_map_file.write(check_map_template)
        check_map_file.close()
        print_message += "'check_map.json' template is SAVED now.\n\n"
        file_touched = True
    print_message += "1. SET 'check_map.json'.\n"
    print_message += "2. RUN the CheckOfChecks using its arguments.\n"
    print(print_message)
    return file_touched


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--json_path',
                        help='set the json path of the customer check map')
    parser.add_argument('-c', '--customer_name',
                        help='select a customer from where checking '
                             'influxdb data')
    parser.add_argument('-v', '--verbose_level',
                        help='verbose the check output')

    cli_args = sys.argv[1:]
    if cli_args:
        args = parser.parse_args()
        json_path = args.json_path if args.json_path else ''
        customer_name = args.customer_name if args.customer_name else False
        verbose_level = int(args.verbose_level) if args.verbose_level else 1
        if customer_name:
            check_customer_influxdb_checks(customer_name,
                                           json_path,
                                           verbose_level)
        else:
            check_customers_influxdb_checks(json_path,
                                            verbose_level)
    else:
        # print(CustomerData('<customer_name>'))
        # print(CustomerInfluxDBData('<customer_name>'))
        # print(CustomerInfluxDBCheck('<customer_name>'))
        # print(CustomersInfluxDBChecks(verbose_level=2))

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

        # check_customer_influxdb_checks('<customer_name>')
        # check_customers_influxdb_checks()

        set_check_map()
        parser.print_help()


if __name__ == '__main__':
    main()
