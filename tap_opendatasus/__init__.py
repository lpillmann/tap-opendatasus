import calendar
import json
import os
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


REQUIRED_CONFIG_KEYS = ["year_month", "state_abbrev"]
CONFIG = {
    "year_month": None,
    "state_abbrev": None,
    "extract_until_date": None,
}
STATE = {}
LOGGER = singer.get_logger()
DATE_FORMAT = "%Y-%m-%d"

# TODO: move to config
ES_HOST = "https://imunizacao-es.saude.gov.br"
ES_INDEX = "desc-imunizacao"
# Public credentials provided by the Brazilian Government
USER = "imunizacao_public"
PASSWORD = "qlto5t&7r_@+#Tlstigi"


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """Load schemas from schemas folder"""
    schemas = {}
    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def get_month_end_date(year_month: str) -> str:
    """Receives year month e.g. 2021-01-01 and returns 2021-01-31"""
    year, month, _ = year_month.split("-")
    _, last_day = calendar.monthrange(int(year), int(month))
    return f"{year}-{month}-{last_day}"


def is_within_month(date: str, year_month: str) -> bool:
    """Checks if date is within given month"""
    month_end_date = get_month_end_date(year_month)
    return all(
        [
            dt.strptime(date, DATE_FORMAT) >= dt.strptime(year_month, DATE_FORMAT),
            dt.strptime(date, DATE_FORMAT) <= dt.strptime(month_end_date, DATE_FORMAT),
        ]
    )


def query_vaccinations(state_abbrev, from_date, to_date):
    """
    Query Elasticsearch endpoint by state and date range (from_date is inclusive, to_date is not)
    Returns Elasticsearch Search object (generator)
    """
    client = Elasticsearch(hosts=ES_HOST, http_auth=(USER, PASSWORD))
    return (
        Search(using=client, index=ES_INDEX)
        .query("match", estabelecimento_uf=state_abbrev)
        .filter(
            "range",
            **{
                "vacina_dataAplicacao": {
                    "gte": from_date,
                    "lt": to_date,
                }
            },
        )
    )


def sync_vaccinations(state, stream) -> tuple:
    """Sync vaccinations data from Open Data SUS

    Uses Singer state functionality to sync incrementally one day at a time
    for attribute `vacina_dataAplicacao` (vaccination date).

    When state is null, starts from the first day of the month according to `year_month`
    passed in config.

    """
    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=stream.schema.to_dict(),
        key_properties=stream.key_properties,
    )

    year_month = CONFIG.get("year_month")
    state_abbrev = CONFIG.get("state_abbrev")
    extract_until_date = CONFIG.get("extract_until_date")

    # Parse state_abbrev_from_date string
    state_abbrev_from_date = state["bookmarks"][stream.tap_stream_id].get(
        "state_abbrev_from_date"
    )
    if state_abbrev_from_date:
        _, from_date = state_abbrev_from_date.split("|")
        LOGGER.info(f"Tap state successfully read, setting from_date to {from_date}")
    else:
        # If state is None, start from first day of the month
        LOGGER.info(
            f"Tap state is None, setting from_date to first day of the month: {year_month}"
        )
        from_date = year_month

    try:
        while dt.strptime(from_date, DATE_FORMAT) <= dt.strptime(extract_until_date, DATE_FORMAT):
            # Sync only within year_month passed
            if not is_within_month(from_date, year_month):
                LOGGER.warning(
                    f"Parameter from_date {from_date} is not within year_month {year_month}. Skipping."
                )
                break

            # Query one day at a time
            to_date = dt.strftime(
                dt.strptime(from_date, DATE_FORMAT) + relativedelta(days=+1),
                DATE_FORMAT,
            )
            LOGGER.info(
                f"\tsync_vaccinations: Getting vaccinations for {state_abbrev} from {from_date} to {to_date}"
            )
            vaccinations_search = query_vaccinations(state_abbrev, from_date, to_date)
            payload = dict()
            for hit in vaccinations_search.scan():
                # Assign one by one to deal with edge cases (e.g. columns with @ prefix and year_month)
                # fmt: off
                payload["document_id"] = hit["document_id"]
                payload["paciente_id"] = hit["paciente_id"]
                payload["paciente_idade"] = hit["paciente_idade"]
                payload["paciente_dataNascimento"] = hit["paciente_dataNascimento"]
                payload["paciente_enumSexoBiologico"] = hit["paciente_enumSexoBiologico"]
                payload["paciente_racaCor_codigo"] = hit["paciente_racaCor_codigo"]
                payload["paciente_racaCor_valor"] = hit["paciente_racaCor_valor"]
                payload["paciente_endereco_coIbgeMunicipio"] = hit["paciente_endereco_coIbgeMunicipio"]
                payload["paciente_endereco_coPais"] = hit["paciente_endereco_coPais"]
                payload["paciente_endereco_nmMunicipio"] = hit["paciente_endereco_nmMunicipio"]
                payload["paciente_endereco_nmPais"] = hit["paciente_endereco_nmPais"]
                payload["paciente_endereco_uf"] = hit["paciente_endereco_uf"]
                payload["paciente_endereco_cep"] = hit["paciente_endereco_cep"]
                payload["paciente_nacionalidade_enumNacionalidade"] = hit["paciente_nacionalidade_enumNacionalidade"]
                payload["estabelecimento_valor"] = hit["estabelecimento_valor"]
                payload["estabelecimento_razaoSocial"] = hit["estabelecimento_razaoSocial"]
                payload["estalecimento_noFantasia"] = hit["estalecimento_noFantasia"]
                payload["estabelecimento_municipio_codigo"] = hit["estabelecimento_municipio_codigo"]
                payload["estabelecimento_municipio_nome"] = hit["estabelecimento_municipio_nome"]
                payload["estabelecimento_uf"] = hit["estabelecimento_uf"]
                payload["vacina_grupoAtendimento_codigo"] = hit["vacina_grupoAtendimento_codigo"]
                payload["vacina_grupoAtendimento_nome"] = hit["vacina_grupoAtendimento_nome"]
                payload["vacina_categoria_codigo"] = hit["vacina_categoria_codigo"]
                payload["vacina_categoria_nome"] = hit["vacina_categoria_nome"]
                payload["vacina_lote"] = hit["vacina_lote"]
                payload["vacina_fabricante_nome"] = hit["vacina_fabricante_nome"]
                payload["vacina_fabricante_referencia"] = hit["vacina_fabricante_referencia"]
                payload["vacina_dataAplicacao"] = hit["vacina_dataAplicacao"]
                payload["vacina_descricao_dose"] = hit["vacina_descricao_dose"]
                payload["vacina_codigo"] = hit["vacina_codigo"]
                payload["vacina_nome"] = hit["vacina_nome"]
                payload["sistema_origem"] = hit["sistema_origem"]
                payload["id_sistema_origem"] = hit["id_sistema_origem"]
                payload["data_importacao_rnds"] = hit["data_importacao_rnds"]
                payload["redshift"] = hit["redshift"]
                payload["timestamp"] = hit["@timestamp"]
                payload["version"] = hit["@version"]
                payload["year_month"] = year_month
                singer.write_records(stream.tap_stream_id, [payload])
                # fmt: on
            from_date = to_date

    except Exception as e:
        LOGGER.fatal(f"Error: {repr(e)}")
        raise e

    return "state_abbrev_from_date", f"{state_abbrev}|{from_date}"


def sync(state, stream):
    """Switch to choose endpoint sync function"""
    return_val = state

    if stream.tap_stream_id == "vaccinations":
        return_val = sync_vaccinations(state, stream)
    return return_val


def do_sync(state, catalog):
    """Sync streams that are selected"""
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info(f"Syncing stream: {stream.tap_stream_id}")

        state_key, state_value = sync(state, stream)

        singer.set_currently_syncing(state, stream.tap_stream_id)
        # Generate updated state bookmarks object
        state = singer.write_bookmark(
            state, stream.tap_stream_id, state_key, state_value
        )
        singer.write_state(state)

    singer.set_currently_syncing(state, None)
    singer.write_state(state)
    LOGGER.info("Sync completed")


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE = {}

    if args.state:
        STATE.update(args.state)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        do_sync(STATE, catalog)


if __name__ == "__main__":
    main()
