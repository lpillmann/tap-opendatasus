import json
import os
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


REQUIRED_CONFIG_KEYS = ["token"]
CONFIG = {
    "token": None,
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

INITIAL_FROM_DATE = '2021-01-01'


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
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
    Elasticsearch request to get vaccines data

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

    The API returns data in the following format

    (TODO)

    It uses Singer state feature to query the endpoint
        - `state_abbrev_from_date`: last Brazilian state and from date queried (assumes alphabetical order for states)
                                 e.g. SC|2021-01-01

    At the end, returns a tuple key/val `state_abbrev_from_date` to be stored as the most recent state.
    """
    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=stream.schema.to_dict(),
        key_properties=stream.key_properties,
    )

    last_synced_state_abbrev, last_synced_from_date = None, None
    
    # Parse slug_reference_date string
    last_synced_state_abbrev_from_date = state["bookmarks"][stream.tap_stream_id].get("state_abbrev_from_date")
    if last_synced_state_abbrev_from_date:
        last_synced_state_abbrev, last_synced_from_date = last_synced_state_abbrev_from_date.split("|")
    
    # State logic to define remaining state abbrevs and dates to query
    all_state_abbrevs = utils.load_json("state_abbreviations.json")["state_abbreviations"]
    remaining_state_abbrevs = all_state_abbrevs
    if last_synced_state_abbrev:
        last_synced_slug_idx = all_state_abbrevs.index(last_synced_state_abbrev)
        # Stays in the last synced state abbrev, since from_date might be not finished yet
        remaining_state_abbrevs = all_state_abbrevs[last_synced_slug_idx:]

    from_date = None
    if last_synced_from_date:
        from_date = last_synced_from_date

    state_abbrev = None
    try:
        for state_abbrev in remaining_state_abbrevs:
            if not (state_abbrev == last_synced_state_abbrev):
                from_date = INITIAL_FROM_DATE

            while datetime.strptime(from_date, DATE_FORMAT) <= datetime.utcnow():
                to_date = datetime.strftime(
                    datetime.strptime(from_date, DATE_FORMAT) + relativedelta(days=+1),
                    DATE_FORMAT,
                )
                LOGGER.info(f"\tsync_vaccines: Getting vaccines for {state_abbrev} from {from_date} to {to_date}")
                vaccines_search = get_vaccines(state_abbrev, from_date, to_date)
                vaccine = dict()
                for hit in vaccines_search.scan():
                    vaccine["estabelecimento_uf"] = hit["estabelecimento_uf"]
                    vaccine["vacina_categoria_nome"] = hit["vacina_categoria_nome"]
                    vaccine["vacina_fabricante_referencia"] = hit["vacina_fabricante_referencia"]
                    vaccine["sistema_origem"] = hit["sistema_origem"]
                    vaccine["id_sistema_origem"] = hit["id_sistema_origem"]
                    vaccine["paciente_endereco_coPais"] = hit["paciente_endereco_coPais"]
                    vaccine["data_importacao_rnds"] = hit["data_importacao_rnds"]
                    vaccine["paciente_endereco_nmMunicipio"] = hit["paciente_endereco_nmMunicipio"]
                    vaccine["estabelecimento_municipio_nome"] = hit["estabelecimento_municipio_nome"]
                    vaccine["vacina_grupoAtendimento_nome"] = hit["vacina_grupoAtendimento_nome"]
                    vaccine["vacina_dataAplicacao"] = hit["vacina_dataAplicacao"]
                    vaccine["estabelecimento_razaoSocial"] = hit["estabelecimento_razaoSocial"]
                    vaccine["vacina_categoria_codigo"] = hit["vacina_categoria_codigo"]
                    vaccine["paciente_idade"] = hit["paciente_idade"]
                    vaccine["estabelecimento_valor"] = hit["estabelecimento_valor"]
                    vaccine["timestamp"] = hit["@timestamp"]
                    vaccine["paciente_id"] = hit["paciente_id"]
                    vaccine["paciente_endereco_cep"] = hit["paciente_endereco_cep"]
                    vaccine["paciente_racaCor_valor"] = hit["paciente_racaCor_valor"]
                    vaccine["vacina_nome"] = hit["vacina_nome"]
                    vaccine["paciente_enumSexoBiologico"] = hit["paciente_enumSexoBiologico"]
                    vaccine["estabelecimento_municipio_codigo"] = hit["estabelecimento_municipio_codigo"]
                    vaccine["paciente_nacionalidade_enumNacionalidade"] = hit["paciente_nacionalidade_enumNacionalidade"]
                    vaccine["vacina_grupoAtendimento_codigo"] = hit["vacina_grupoAtendimento_codigo"]
                    vaccine["vacina_fabricante_nome"] = hit["vacina_fabricante_nome"]
                    vaccine["vacina_codigo"] = hit["vacina_codigo"]
                    vaccine["paciente_endereco_coIbgeMunicipio"] = hit["paciente_endereco_coIbgeMunicipio"]
                    vaccine["redshift"] = hit["redshift"]
                    vaccine["vacina_lote"] = hit["vacina_lote"]
                    vaccine["version"] = hit["@version"]
                    vaccine["document_id"] = hit["document_id"]
                    vaccine["paciente_endereco_uf"] = hit["paciente_endereco_uf"]
                    vaccine["paciente_racaCor_codigo"] = hit["paciente_racaCor_codigo"]
                    vaccine["vacina_descricao_dose"] = hit["vacina_descricao_dose"]
                    vaccine["estalecimento_noFantasia"] = hit["estalecimento_noFantasia"]
                    vaccine["paciente_endereco_nmPais"] = hit["paciente_endereco_nmPais"]
                    vaccine["paciente_dataNascimento"] = hit["paciente_dataNascimento"]
                    vaccine["from_date"] = from_date
                    singer.write_records(stream.tap_stream_id, [vaccine])
                # time.sleep(1)
                from_date = to_date

    except Exception as e:
        LOGGER.fatal(
            f"Error: {repr(e)}"
        )

    return "state_abbrev_from_date", f"{state_abbrev}|{from_date}"


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
