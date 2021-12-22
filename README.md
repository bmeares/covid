# COVID-19 Meerschaum Plugin

This Meerschaum plugin scrapes county-level COVID-19 data for US states. For implemented states (see below), data are fetched directly from health departments, otherwise the New York Times is used as the source.

## Motivation

The [New York Times tracker](https://github.com/nytimes/covid-19-data) is the most complete source for county-level US COVID-19 data, but its distribution via CSV is somewhat cumbersome, and many smaller counties are missing. This plugin makes rountinely ingesting COVID-19 data into your projects a straightforward process.

## Data Sources

This plugin aggregates data from other Meerschaum plugins:

- [California Department of Public Health](https://github.com/bmeares/CA-covid)
- [Colorado Department of Public Health and Environment](https://github.com/bmeares/CO-covid)
- [Georgia Department of Public Health](https://github.com/bmeares/GA-covid)
- [Texas Department of State Health Services](https://github.com/bmeares/TX-covid)
- [New York Times](https://github.com/bmeares/US-covid)

## Usage

### Installation

1. Install [Meerschaum](https://meerschaum.io).

```bash
pip install -U --user meerschaum
```

2. Install the `covid` plugin.

```bash
mrsm install plugin covid
```

### Registration

1. Register a new pipe.

```bash
mrsm register pipe -c plugin:covid -m cases -i sql:local
```

2. When prompted, enter a list of FIPS codes for the counties you want to fetch, for example:

```bash
08031,48113,45043,45045,45007,37107,37021,47157,47147
```

In this example, the data are fetched and stored in the `sql:local` instance, a built-in SQLite database.

### Accessing Your Data

#### Pandas DataFrame

```python
>>> import meerschaum as mrsm
>>> pipe = mrsm.Pipe('plugin:covid', 'cases', instance='sql:local')
>>> ### Optionally specify begin / end datetimes, params, etc.
>>> df = pipe.get_data(begin='2021-01-01', params={'fips': ['53053', '45007', '37119']})
>>> df
           date   fips   cases
0    2021-12-21  37119  169288
1    2021-12-21  45007   38888
2    2021-12-21  53053  103837
3    2021-12-20  37119  168820
4    2021-12-20  45007   38822
...         ...    ...     ...
1060 2021-01-02  45007   12297
1061 2021-01-02  37119   64911
1062 2021-01-01  53053   28056
1063 2021-01-01  45007   12054
1064 2021-01-01  37119   64167

[1065 rows x 3 columns]
>>> 
```

#### Relational Database

The instructions above use the built-in SQLite database (`sql:local`) stored at `~/.config/meerschaum/sqlite/mrsm_local.db` (Windows: `%APPDATA%\Meerschaum\sqlite\mrsm_local.db`), but you can use any database you want (the states' data are cached in `~/.config/meerschaum/sqlite/covid.db`).

If you have Docker installed, run `mrsm stack up -d db` to bring up `sql:main`, a TimescaleDB (PostgreSQL) instance.

You can also add your own database as an instance with `mrsm bootstrap connector`, though keep in mind that using a database as an instance creates the tables `users`, `pipes`, and `plugins`.

## Contributing

The best way to contribute is to [write a Meerschaum plugin](https://meerschaum.io/reference/plugins/writing-plugins/) for your state. The main requirement is that your `fetch()` function must return a DataFrame (or dictionary of lists) with the columns date (datetime), fips (str), and cases (int). Check out the [California Meerschaum plugin](https://meerschaum.io/reference/plugins/writing-plugins/) for reference (e.g. how to filter by FIPS).

After testing and publising a plugin for your state, please open a PR on this repository to add your plugin to the `required` list. Please use the format `<STATE ABBREVIATION>-covid` when naming your plugin (e.g. `CO-covid` for Colorado).
