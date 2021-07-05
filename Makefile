sync-csv:
	python tap_opendatasus/__init__.py --config config.json --catalog catalog.json --state state.json | ~/.virtualenvs/target-csv/bin/target-csv --config csv_config.json >> state.json
	tail -1 state.json > state.json.tmp && mv state.json.tmp state.json

sync-s3-csv:
	python tap_opendatasus/__init__.py --config config.json --catalog catalog.json --state state.json | ~/.virtualenvs/pipelinewise-target-s3-csv/bin/target-s3-csv --config s3_csv_config.json >> state.json
	tail -1 state.json > state.json.tmp && mv state.json.tmp state.json