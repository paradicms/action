#!/usr/bin/env python3

import dataclasses
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import sys
from typing import Optional

from paradicms_etl.extractors.markdown_directory_extractor import (
    MarkdownDirectoryExtractor,
)
from paradicms_etl._loader import _Loader
from paradicms_etl.loaders.rdf_file_loader import RdfFileLoader
from paradicms_etl._pipeline import _Pipeline
from paradicms_etl.transformers.markdown_directory_transformer import (
    MarkdownDirectoryTransformer,
)


class Action:
    @dataclass(frozen=True)
    class Inputs:
        id: str
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
            if "id" not in kwds:
                kwds["id"] = os.environ["GITHUB_REPOSITORY"].rsplit("/", 1)[-1]
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
        self.__pipeline_id = self.__inputs.id

    def __create_loader(self) -> _Loader:
        output_format = self.__inputs.output_format.lower()
        if output_format.endswith("-rdf"):
            rdf_format = output_format[: -len("-rdf")]
            output_data_path = Path(self.__inputs.output_data).absolute()
            if output_data_path.is_dir():
                rdf_file_path = output_data_path / ("data." + rdf_format)
            else:
                rdf_file_path = output_data_path
                self.__mkdir(rdf_file_path.parent)
            self.__logger.info(
                "RDF file loader: format=%s, file path=%s", rdf_format, rdf_file_path
            )
            return RdfFileLoader(
                file_path=rdf_file_path,
                format=rdf_format,
                pipeline_id=self.__pipeline_id,
            )
        else:
            raise NotImplementedError(self.__inputs.output_format)

    def __create_markdown_directory_pipeline(self, *, loader: _Loader) -> _Pipeline:
        extracted_data_dir_path = Path(self.__inputs.input_data)
        if not extracted_data_dir_path.is_dir():
            raise ValueError(
                f"Markdown directory {extracted_data_dir_path} does not exist"
            )

        return _Pipeline(
            extractor=MarkdownDirectoryExtractor(
                extracted_data_dir_path=extracted_data_dir_path,
                pipeline_id=self.__pipeline_id,
            ),
            id=self.__pipeline_id,
            loader=loader,
            transformer=MarkdownDirectoryTransformer(
                pipeline_id=self.__pipeline_id,
            ),
        )

    def __create_pipeline(self) -> _Pipeline:
        loader = self.__create_loader()
        if self.__inputs.input_format in ("markdown", "markdown_directory"):
            return self.__create_markdown_directory_pipeline(loader=loader)
        else:
            raise NotImplementedError(self.__inputs.input_format)

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
        pipeline = self.__create_pipeline()
        pipeline.extract_transform_load()


if __name__ == "__main__":
    Action().run()
