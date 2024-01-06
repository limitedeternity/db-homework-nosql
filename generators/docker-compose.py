from pathlib import Path

from __main__ import BinderBase


class TemplateBinder(BinderBase):
    @staticmethod
    def get_template_path() -> Path:
        return Path(__file__).parent / "templates" / "docker-compose.yml"

    @staticmethod
    def get_output_path() -> Path:
        return Path(__file__).parent.parent / "docker-compose.yml"

    @staticmethod
    def get_template_config() -> dict:
        return {
            "KAFKA_CLUSTER_NODES": 3
        }
