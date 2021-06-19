#!/usr/bin/env python3

from configargparse import ArgParser
import dataclasses
from dataclasses import dataclass
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

from paradicms_etl.extractors.markdown_directory_extractor import (
    MarkdownDirectoryExtractor,
)
from paradicms_etl._loader import _Loader
from paradicms_etl.loaders.gui.fs_gui_deployer import FsGuiDeployer
from paradicms_etl.loaders.gui.gui_loader import GuiLoader
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
        def from_args(cls):
            argument_parser = ArgParser()
            argument_parser.add_argument(
                "-c", is_config_file=True, help="config file path"
            )
            for field in dataclasses.fields(cls):
                argument_parser.add_argument(
                    "--" + field.name.replace("_", "-"),
                    env_var="INPUT_" + field.name.upper(),
                    required=field.name != "debug",
                )
            args = argument_parser.parse_args()
            kwds = vars(args).copy()
            for ignore_key in ("c",):
                kwds.pop(ignore_key, None)
            if "id" not in kwds:
                kwds["id"] = os.environ["GITHUB_REPOSITORY"].rsplit("/", 1)[-1]
            return cls(**kwds)

    def __init__(self, *, inputs: Inputs, temp_dir_path: Path):
        self.__inputs = inputs
        if self.__inputs.debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__logger.debug("inputs: %s", self.__inputs)
        self.__pipeline_id = self.__inputs.id
        self.__temp_dir_path = temp_dir_path

    def __create_gui_loader(self, *, output_format):
        if output_format == "gui":
            gui = "material-ui-union"
        elif output_format.endswith("-gui"):
            gui = output_format[: -len("-gui")]
        else:
            raise NotImplementedError

        gui_deploy_dir_path = Path(self.__inputs.output_data).absolute()

        for gui_dir_path in (
            Path(gui).absolute(),
            Path("/paradicms") / "gui" / "app" / gui,
        ):
            if gui_dir_path.is_dir():
                self.__logger.debug("gui_dir_path %s exists, using", gui_dir_path)
                gui = gui_dir_path
                break
            else:
                self.__logger.debug("gui_dir_path %s does not exist", gui_dir_path)

        self.__logger.info(
            "GUI loader: gui=%s, deploy path=%s", gui, gui_deploy_dir_path
        )

        return GuiLoader(
            deployer=FsGuiDeployer(gui_deploy_dir_path=gui_deploy_dir_path),
            loaded_data_dir_path=self.__temp_dir_path,
            gui=gui,
            pipeline_id=self.__pipeline_id,
        )

    def __create_loader(self) -> _Loader:
        output_format = self.__inputs.output_format.lower()
        if output_format == "gui" or output_format.endswith("-gui"):
            return self.__create_gui_loader(output_format=output_format)
        elif output_format == "rdf" or output_format.endswith("rdf"):
            return self.__create_rdf_file_loader(output_format=output_format)
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

        input_format = self.__inputs.input_format.lower()
        if self.__inputs.input_format in ("markdown", "markdown_directory"):
            return self.__create_markdown_directory_pipeline(loader=loader)
        else:
            raise NotImplementedError(self.__inputs.input_format)

    def __create_rdf_file_loader(self, *, output_format: str) -> RdfFileLoader:
        if output_format == "rdf":
            rdf_format = RdfFileLoader.FORMAT_DEFAULT
        elif output_format.endswith("-rdf"):
            rdf_format = output_format[: -len("-rdf")]
        else:
            raise NotImplementedError

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

    def __mkdir(self, dir_path: Path) -> None:
        if dir_path.is_dir():
            self.__logger.debug("directory %s already exists", dir_path)
            return
        elif dir_path.exists():
            raise IOError("%s already exists and is not a directory", dir_path)
        else:
            dir_path.mkdir(parents=True, exist_ok=True)
            self.__logger.debug("created directory %s", dir_path)

    @classmethod
    def main(cls, inputs: Optional[Inputs] = None):
        if inputs is None:
            inputs = cls.Inputs.from_args()
        with TemporaryDirectory() as temp_dir:
            cls(inputs=inputs, temp_dir_path=Path(temp_dir)).__main()

    def __main(self):
        pipeline = self.__create_pipeline()
        pipeline.extract_transform_load()


if __name__ == "__main__":
    Action.main()
