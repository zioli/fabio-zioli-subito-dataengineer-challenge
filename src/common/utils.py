import logging
from logging import Logger
import argparse
import json



def exclude_columns(columns:list, columns_to_remove:list) -> list:
    return [c for c in columns if c not in columns_to_remove]

def get_logger(name=__name__, level=logging.INFO)->Logger:
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(name)

def get_parameter(logger:Logger):
    parser = argparse.ArgumentParser(description="Subito Spark Job Parameters")
    default_input = {
        "city_attributes": "data/raw/city_attributes.csv",
        "humidity": "data/raw/humidity.csv",
        "pressure": "data/raw/pressure.csv",
        "temperature": "data/raw/temperature.csv",
        "weather_description": "data/raw/weather_description.csv",
    }
    parser.add_argument(
        '--config',
        type=str,
        default=json.dumps(default_input),
        help='JSON string for job configuration'
    )
    args = parser.parse_args()
    try:
        j = json.loads(args.config)

        if j.get("city_attributes", None) is None:
            raise ValueError("Missing city_attributes parameter")

        if j.get("humidity", None) is None:
            raise ValueError("Missing humidity parameter")

        if j.get("pressure", None) is None:
            raise ValueError("Missing pressure parameter")

        if j.get("temperature", None) is None:
            raise ValueError("Missing temperature parameter")

        if j.get("weather_description", None) is None:
            raise ValueError("Missing weather_description parameter")

        return j
    except json.decoder.JSONDecodeError as e:
        logger.error(f"Invalid JSON format for --config: {args.config}")
        raise