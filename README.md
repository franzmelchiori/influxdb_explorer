# CheckOfChecks

1. **RUN** the *CheckOfChecks* with *no arguments* to touch `check_map.json`
2. **SET** `check_map.json`
3. **RUN** the *CheckOfChecks* using its arguments

***

usage:

* `python influxdb_explorer.py` `[-h]` `[-p JSON_PATH]` `[-c CUSTOMER_NAME]` `[-v VERBOSE_LEVEL]`

optional arguments:
* `-h`, `--help`
    * show this help message and exit
* `-p JSON_PATH`, `--json_path JSON_PATH`
    * set the json path of the check map
* `-c CUSTOMER_NAME`, `--customer_name CUSTOMER_NAME`
    * select a customer from where checking influxdb data (default: `all`)
* `-v VERBOSE_LEVEL`, `--verbose_level VERBOSE_LEVEL`
    * verbose the check output (default: `1`)
