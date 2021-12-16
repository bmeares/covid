#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch county-level COVID-19 data for all 50 US states.
"""

from __future__ import annotations
from typing import Union, Optional, Any, List, Dict
import meerschaum as mrsm

__version__: str = '0.0.2'
implemented_states: List[str] = ['CA', 'CO', 'GA', 'TX']
required: List[str] = (
    ['pandas', 'duckdb', 'plugin:nytcovid']
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
    from meerschaum.utils.pool import get_pool
    from functools import partial
    from meerschaum.actions import actions
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

    ### This is a workaround because the other plugins have interactive `register()` functions.
    for state, p in states_pipes.items():
        if p.get_id(debug=debug) is None:
            p.instance_connector.register_pipe(p, debug=debug)

    _kw = kw.copy()
    _kw.update(dict(
        action = ['pipes'],
        mrsm_instance = 'sql:local',
        connector_keys = [p.connector_keys for s, p in states_pipes.items()],    
        metric_keys = [p.metric_key for s, p in states_pipes.items()],    
        location_keys = [p.location_key for s, p in states_pipes.items()],    
        debug = debug,

    ))
    actions['sync'](**_kw)

    dfs = [
        df[dtypes.keys()].astype(dtypes) for df in pool.map(
            partial(_get_df, **kw), [pipe for state, pipe in states_pipes.items()]
        ) if df is not None
    ]

    return pd.concat(dfs) if dfs else None


def _get_df(pipe: mrsm.Pipe, debug: bool = False, **kw) -> Union['pd.DataFrame', None]:
    return pipe.get_data(debug=debug, **kw)
