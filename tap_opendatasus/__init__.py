import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from utils import get_month_end_date


REQUIRED_CONFIG_KEYS = ["year_month", "state_abbrev"]
CONFIG = {
    "year_month": None,
    "state_abbrev": None,
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


def get_vaccines(state_abbrev, from_date, to_date):
    """
    Query Elasticsearch endpoint by state and date range
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


def sync_vaccines(state, stream) -> tuple:
    """Sync vaccines data from Open Data SUS

    Note: Singer state functionality is currently disabled in favor of the Shell script that passes year month
    and (Brazilian) state abbreviation. So the extraction sequence is determined externally as of now.

    """
    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=stream.schema.to_dict(),
        key_properties=stream.key_properties,
    )

    year_month = CONFIG.get("year_month")
    state_abbrev = CONFIG.get("state_abbrev")
    month_end_date = get_month_end_date(year_month)
    from_date = year_month  # First day of the month
    try:
        while datetime.strptime(from_date, DATE_FORMAT) <= datetime.strptime(
            month_end_date, DATE_FORMAT
        ):
            to_date = datetime.strftime(
                datetime.strptime(from_date, DATE_FORMAT) + relativedelta(days=+1),
                DATE_FORMAT,
            )
            LOGGER.info(
                f"\tsync_vaccines: Getting vaccines for {state_abbrev} from {from_date} to {to_date}"
            )
            vaccines_search = get_vaccines(state_abbrev, from_date, to_date)
            payload = dict()
            for hit in vaccines_search.scan():
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

    return "state_abbrev_year_month", f"{state_abbrev}|{from_date}"


def sync(state, stream):
    """Switch to choose endpoint sync function"""
    return_val = state

    if stream.tap_stream_id == "vaccines":
        return_val = sync_vaccines(state, stream)
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
