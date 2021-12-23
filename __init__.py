#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch county-level COVID-19 data for all 50 US states.
"""

from __future__ import annotations
from typing import Union, Optional, Any, List, Dict
from meerschaum.config._paths import SQLITE_RESOURCES_PATH
CACHE_DB_PATH = SQLITE_RESOURCES_PATH / 'covid.db'
import meerschaum as mrsm

__version__: str = '0.1.3'
implemented_states: List[str] = ['CA', 'CO', 'GA', 'TX', 'US',]
required: List[str] = (
    ['pandas', 'duckdb',]
    + [f'plugin:{state}-covid' for state in implemented_states]
)
dtypes: Dict[str, Any] = {
    'date': 'datetime64[ms]',
    'fips': str,
    'cases': int,
}

def register(pipe: mrsm.Pipe, **kw) -> Dict[str, Any]:
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
        workers: Optional[int] = None,
        **kw
    ) -> Union['pd.DataFrame', None]:
    import duckdb
    import pandas as pd
    from .fips import states_df
    from functools import partial
    from meerschaum.actions import actions
    from meerschaum.utils.pool import get_pool
    from meerschaum.utils.packages import run_python_package
    import subprocess
    import sys
    import json
    pool = get_pool(workers=workers)
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

    ### Store the states' pipes in a new SQLite database.
    conn_attrs = {'flavor': 'sqlite', 'database': str(CACHE_DB_PATH)}

    states_pipes = {
        state: mrsm.Pipe(
            f'plugin:{state}-covid', 'cases', state,
            instance = mrsm.get_connector('sql', '_covid', **conn_attrs),
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

    ### This is a workaround because the other plugins have interactive `register()` functions.
    for state, p in states_pipes.items():
        if p.get_id(debug=debug) is None:
            p.instance_connector.register_pipe(p, debug=debug)

    ### Open a subprocesses so we can let the synchronization engine handle concurrency.
    cmds = (
        [sys.executable, '-m', 'meerschaum']
        + ['sync', 'pipes', ]
        + ['-c', ] + [p.connector_keys for s, p in states_pipes.items()]
        + ['-m', ] + [p.metric_key for s, p in states_pipes.items()]
        + ['-l', ] + [p.location_key for s, p in states_pipes.items()]
        + (['--debug'] if debug else [])
        + ['-i', 'sql:_covid']
        + (['-w', str(workers)] if workers is not None else [])
    )
    ### Patch our temporary database config onto the regular config so it appears "registered".
    mrsm_env = {
        'MRSM_PATCH': json.dumps({
            'meerschaum': {
                'connectors': {
                    'sql': {
                        '_covid': conn_attrs
                    },
                },
            },
        })
    }
    with subprocess.Popen(cmds, env=mrsm_env) as proc:
        proc.wait()
        success = proc.wait()
    if success != 0:
        raise Exception("Failed to sync states' pipes.")

    dfs = [
        df[dtypes.keys()].astype(dtypes) for df in pool.map(
            partial(_get_df, **kw), [p for s, p in states_pipes.items()]
        ) if df is not None
    ] if pool is not None else [
        df[dtypes.keys()].astype(dtypes) for df in [
            _get_df(p, debug=debug) for s, p in states_pipes.items()
        ] if df is not None
    ]
    return pd.concat(dfs)[dtypes.keys()].astype(dtypes) if dfs else None


def _get_df(pipe: mrsm.Pipe, debug: bool = False, **kw) -> Union['pd.DataFrame', None]:
    return pipe.get_data(debug=debug, **kw)
