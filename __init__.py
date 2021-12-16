#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch county-level COVID-19 data for all 50 US states.
"""

from __future__ import annotations
import meerschaum as mrsm
__version__ = '0.0.1'

states = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
]
implemented_states = ['CA', 'CO', 'GA', 'TX']

required = (
    ['pandas', 'duckdb', 'plugin:nytcovid']
    + [f'plugin:{state}-covid' for state in implemented_states]
)


def register(pipe: mrsm.Pipe, **kw):
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.prompt import prompt, yes_no
    while True:
        fips_str = prompt("Please enter a list of FIPS codes separated by commas:")
        fips = fips_str.replace(' ', '').split(',')
        question = "Is this correct?"
        for f in fips:
            question += f"\n  - {f}"
        question += '\n'
        if not fips or not yes_no(question):
            continue
        break

    return {
        'columns': {
            'datetime': 'date',
            'id': 'fips',
            'value': 'cases'
        },
        'covid': {
            'fips': fips,
        },
    }


def fetch(
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kw
    ):
    import duckdb
    import pandas as pd
    from .fips import states_df
    from meerschaum.utils.warnings import warn
    fips_list = pipe.parameters['covid']['fips']
    states_fips = {}
    for f in fips_list:
        query = f"SELECT postal_code FROM states_df WHERE state_fips = '{f[:2]}'"
        _state = duckdb.query(query).fetchone()[0]
        ### If we don't have a plugin, fall back to `nytcovid`.
        state = _state if _state in implemented_states else 'US'
        if state not in states_fips:
            states_fips[state] = []
        states_fips[state].append(f)

    states_pipes = {
        state: mrsm.Pipe(
            f'plugin:{state}-covid', 'cases', state,
            instance = 'sql:local',
            parameters = {
                'columns': {
                    'datetime': 'date',
                    'id': 'fips',
                    'value': 'cases',
                },
                f'{state}-covid': {
                    'fips': fips,
                },
            }
        ) for state, fips in states_fips.items()
    }

    dtypes = {
        'date': 'datetime64[ms]',
        'fips': str,
        'cases': int,
    }
    dfs = []
    for state, pipe in states_pipes.items():
        ### This is a workaround because the other plugins have interactive `register()` functions.
        if pipe.get_id(debug=debug) is None:
            pipe.instance_connector.register_pipe(pipe, debug=debug)
        try:
            dfs.append(
                pd.DataFrame(pipe.fetch(debug=debug, **kw))[dtypes].astype(dtypes)
            )
        except Exception as e:
            warn(f"Error fetching for state '{state}':\n{e}")
    return pd.concat(dfs)


def _get_df(pipe)
