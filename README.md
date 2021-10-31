# owid-catalog

_A Pythonic API for working with OWID's data catalog._

Status: experimental, APIs likely to change

## Quickstart

Install with `pip install owid-catalog`. Then you can begin exploring the experimental data catalog:

```python
from owid import catalog

# look for Covid-19 data, return a data frame of matches
catalog.find('covid')

# load Covid-19 data from the Our World In Data namespace as a data frame
df = catalog.find('covid', namespace='owid').load()
```

## Development

You need Python 3.9+, `poetry` and `make` installed. Clone the repo, then you can simply run:

```
# run all unit tests and CI checks
make test

# watch for changes, then run all checks
make watch
```

## Data types

### Catalog

A catalog is an arbitrarily deep folder structure containing datasets inside. It can be local on disk, or remote.

#### Load the remote catalog

```python
# find the default OWID catalog and fetch the catalog index over HTTPS
cat = RemoteCatalog()

# get a list of matching tables in different datasets
matches = cat.find('population')

# fetch a data frame for a specific match over HTTPS
t = cat.find_one('population', namespace='gapminder')
```

### Datasets

A dataset is a folder of tables containing metadata about the overall collection.

- Metadata about the dataset lives in `index.json`
- All tables in the folder must share a common format (CSV or Feather)

#### Create a new dataset

```python
# make a folder and an empty index.json file
ds = Dataset.create('/tmp/my_data')
```

```python
# choose CSV instead of feather for files
ds = Dataset.create('/tmp/my_data', format='csv')
```

#### Add a table to a dataset

```python
# serialize a table using the table's name and the dataset's default format (feather)
# (e.g. /tmp/my_data/my_table.feather)
ds.add(table)
```

#### Remove a table from a dataset

```python
ds.remove('table_name')
```

#### Access a table

```python
# load a table including metadata into memory
t = ds['my_table']
```

#### List tables

```python
# the length is the number of datasets discovered on disk
assert len(ds) > 0
```

```python
# iterate over the tables discovered on disk
for table in ds:
    do_something(table)
```

#### Add metadata

```python
# you need to manually save your changes
ds.title = "Very Important Dataset"
ds.description = "This dataset is a composite of blah blah blah..."
ds.save()
```

#### Copy a dataset

```python
# copying a dataset copies all its files to a new location
ds_new = ds.copy('/tmp/new_data_path')

# copying a dataset is identical to copying its folder, so this works too
shutil.copytree('/tmp/old_data', '/tmp/new_data_path')
ds_new = Dataset('/tmp/new_data_path')
```
### Tables

Tables are essentially pandas DataFrames but with metadata. All operations on them occur in-memory, except for loading from and saving to disk. On disk, they are represented by tabular file (feather or CSV) and a JSON metadata file.

#### Make a new table

```python
# same API as DataFrames
t = Table({
    'gdp': [1, 2, 3],
    'country': ['AU', 'SE', 'CH']
}).set_index('country')
```

#### Add metadata about the whole table

```python
t.title = 'Very important data'
```

#### Add metadata about a field

```python
t.gdp.description = 'GDP measured in 2011 international $'
t.sources = [
    Source(title='World Bank', url='https://www.worldbank.org/en/home')
]
```

#### Add metadata about all fields at once

```python
# sources and licenses are actually stored a the field level
t.sources = [
    Source(title='World Bank', url='https://www.worldbank.org/en/home')
]
t.licenses = [
    License('CC-BY-SA-4.0', url='https://creativecommons.org/licenses/by-nc/4.0/')
]
```

#### Save a table to disk

```python
# save to /tmp/my_table.feather + /tmp/my_table.meta.json
t.to_feather('/tmp/my_table.feather')

# save to /tmp/my_table.csv + /tmp/my_table.meta.json
t.to_csv('/tmp/my_table.csv')
```

#### Load a table from disk

These work like normal pandas DataFrames, but if there is also a `my_table.meta.json` file, then metadata will also get read. Otherwise it will be assumed that the data has no metadata:

```python
t = Table.read_feather('/tmp/my_table.feather')

t = Table.read_csv('/tmp/my_table.csv')
```


## Changelog

- `v0.2.2`
    - Repack frames to compact dtypes on `Table.to_feather()`
- `v0.2.1`
    - Fix key typo used in version check
- `v0.2.0`
    - Copy dataset metadata into tables, to make tables more traceable
    - Add API versioning, and a requirement to update if your version of this library is too old
- `v0.1.1`
    - Add support for Python 3.8
- `v0.1.0`
    - Initial release, including searching and fetching data from a remote catalog
