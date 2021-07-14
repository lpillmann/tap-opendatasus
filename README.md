# tap-opendatasus

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Open Data SUS](https://opendatasus.saude.gov.br/)
- Extracts the following resources:
  - [Vaccinations: _Campanha Nacional de Vacinação contra Covid-19_](https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2018 Stitch


## Install

### Install this tap
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
```
### Install S3-CSV target
```bash
python3 -m venv ~/.virtualenvs/pipelinewise-target-s3-csv
source ~/.virtualenvs/pipelinewise-target-s3-csv/bin/activate
pip install git+https://github.com/lpillmann/pipelinewise-target-s3-csv.git
deactivate
```
### Install CSV target
```bash
python3 -m venv ~/.virtualenvs/target-csv
source ~/.virtualenvs/target-csv/bin/activate
pip install target-csv
deactivate
```

## How to use
Run tap & target with:
```bash
source venv/bin/activate
bash run.sh <year-month> <state-abbrev> <load-mode>
```
where:
- `<year-month>` is the month in which vaccinations were applied (e.g. `2021-01-01`) 
- `<state-abbrev>` is a valid Brazilian state abbreviation (e.g. `SP` for São Paulo)
- `<load-mode>` (optional) when `replace` the destination bucket will be emptied before uploading new data. If not passed, CSV file will be added without deleting existing ones.

Data will be loaded into S3 bucket in partitioned fashion like `s3-bucket/.../year_month=2021-01-01/estabelecimento_uf=SP/vaccinations_*.csv`.

### Why we need Shell scripts
The Singer target in use doesn't allow to set the S3 object key dynamically in Python. This is needed in order to place the CSV into partitioned "directories" for each set of parameters used in the extraction. The only way to do that is to change the JSON configuration file passed when running the extraction command. The `run.sh` script enables dynamic creation of the configuration files for the tap and target based or parameters passed. The other Shell scripts (`run_all.sh` and `run_state_abrev.sh`) call the first script passing parameters from lists saved as TXT files (`year_month` and `state_abbrev` values). Finally, `run_local.sh` is used only for development purposes with local CSV target.

## Guide to add new endpoint

- Check its documentation
- Create schema JSON to reflect each of the response field names & types
- Populate catalog JSON with the schema contents. Add metadata accordingly (see existing examples and replicate)
- Determine the endpoint scan logic (e.g. query by date) and add the bookmark key (column name) and value (initial state) in state JSON
- Create a `sync_<endpoint>` function with the scan logic as needed
- Call the function created above under a new `elif` clause in `sync` function
- Leave only the new endpoint as `"selected": true` (need to change only the topmost metadata object with `"breadcrumb": []`) in `catalog.json`
- Run tap & target to test
- Check logs & results in local CSV
