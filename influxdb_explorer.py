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


class DataCustomer:
    def __init__(self, customer_name):
        self.customer_name = customer_name
        load_json(self.customer_name)


def load_json(file_name):
    try:
        json_file = open('{0}'.format(file_name))
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
    load_json('bossard_influxdb_testcase_map.json')
