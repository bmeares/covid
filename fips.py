#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Map fips to states.
"""

from __future__ import annotations
from typing import Dict
import duckdb
import pathlib
import textwrap
STATES_CSV_PATH = pathlib.Path(__file__).parent / 'states.csv'

states_df = duckdb.query(textwrap.dedent("""
    SELECT *
    FROM read_csv(
        '""" + str(STATES_CSV_PATH) + """',
        header = True,
        columns = {
            'state_name': 'VARCHAR',
            'postal_code': 'VARCHAR',
            'state_fips': 'VARCHAR'
        }
    );
""")).df()

