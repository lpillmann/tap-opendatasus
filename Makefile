sync-csv:
	python tap_opendatasus/__init__.py --config config.json --catalog catalog.json --state state.json | ~/.virtualenvs/target-csv/bin/target-csv --config csv_config.json >> state.json
	tail -1 state.json > state.json.tmp && mv state.json.tmp state.json