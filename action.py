#!/usr/bin/env python3

import dataclasses
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import sys
from typing import Optional

from paradicms_etl._loader import _Loader
from paradicms_etl.loaders.rdf_file_loader import RdfFileLoader


class Action:
    @dataclass(frozen=True)
    class Inputs:
        input_data: str
        input_format: str
        output_data: str
        output_format: str
        debug: str = ""

        @classmethod
        def from_environment(cls):
            kwds = {}
            for field in dataclasses.fields(cls):
                environ_key = "INPUT_" + field.name.upper()
                environ_value = os.environ.get(environ_key)
                if environ_value:
                    environ_value = environ_value.strip()
                    if environ_value:
                        kwds[field.name] = environ_value
            return cls(**kwds)

    def __init__(self, inputs: Optional[Inputs] = None):
        if inputs is None:
            inputs = Action.Inputs.from_environment()
        self.__inputs = inputs
        if self.__inputs.debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__logger.debug("inputs: %s", self.__inputs)

    def __create_loader(self) -> _Loader:
        if self.__inputs.output_format.endswith("-rdf"):
            rdf_file_path = Path(self.__inputs.output_data)
            rdf_format = self.inputs.output_format[: -len("-rdf")]
            self.__mkdir(rdf_file_path.parent)
            self.__logger(
                "RDF file loader: format=%s, file path=%s", rdf_format, rdf_file_path
            )
            return RdfFileLoader(file_path=rdf_file_path, format=rdf_format)
        else:
            raise NotImplementedError(self.__inputs.output_format)

    def __mkdir(self, dir_path: Path) -> None:
        if dir_path.is_dir():
            self.__logger.debug("directory %s already exists", dir_path)
            return
        elif dir_path.exists():
            raise IOError("%s already exists and is not a directory", dir_path)
        else:
            dir_path.mkdir(parents=True, exist_ok=True)
            self.__logger.debug("created directory %s", dir_path)

    def run(self):
        pass


if __name__ == "__main__":
    Action().run()
