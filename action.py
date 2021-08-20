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
from paradicms_gui.deployers.fs_deployer import FsDeployer
from paradicms_gui.loaders.gui_loader import GuiLoader
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
        dev: bool = False
        gui_base_url_path: str = ""

        @classmethod
        def from_args(cls):
            argument_parser = ArgParser()
            argument_parser.add_argument(
                "-c", is_config_file=True, help="config file path"
            )
            for field in dataclasses.fields(cls):
                if field.name == "dev":
                    continue
                argument_parser.add_argument(
                    "--" + field.name.replace("_", "-"),
                    env_var="INPUT_" + field.name.upper(),
                    required=field.default == dataclasses.MISSING,
                )
                field.default
            # The dev argument can only be supplied manually from the command line.
            # It makes no sense to run the Next.js dev server ("next dev") in the GitHub Action.
            argument_parser.add_argument("--dev", action="store_true")
            args = argument_parser.parse_args()
            kwds = {
                key: value
                for key, value in vars(args).items()
                if isinstance(value, str) and value.strip()
            }
            if args.dev:
                kwds["dev"] = True
            for ignore_key in ("c",):
                kwds.pop(ignore_key, None)
            if "id" not in kwds:
                kwds["id"] = os.environ["GITHUB_REPOSITORY"].rsplit("/", 1)[-1]
            return cls(**kwds)

        def __post_init__(self):
            for field in dataclasses.fields(self):
                if field.name in ("debug", "dev"):
                    continue
                value = getattr(self, field.name)
                if not value.strip():
                    raise ValueError("empty/blank " + field.name)

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
            app = "material-ui-union"
        elif output_format.endswith("-gui"):
            app = output_format[: -len("-gui")]
        else:
            raise NotImplementedError

        deploy_dir_path = Path(self.__inputs.output_data).absolute()

        for app_dir_path in (
            Path(app).absolute(),
            Path("/paradicms") / "gui" / "app" / app,
        ):
            if app_dir_path.is_dir():
                self.__logger.debug("app_dir_path %s exists, using", app_dir_path)
                app = app_dir_path
                break
            else:
                self.__logger.debug("app_dir_path %s does not exist", app_dir_path)

        self.__logger.info(
            "GUI loader: app=%s, deploy path=%s, base URL path=%s",
            app,
            deploy_dir_path,
            self.__inputs.gui_base_url_path,
        )

        return GuiLoader(
            app=app,
            base_url_path=self.__inputs.gui_base_url_path,
            deployer=FsDeployer(
                # We're running in an environment that's never been used before, so no need to archive
                archive=False,
                # We're also running in Docker, which usually means that the GUI's out directory is on a different mount
                # than the directory we're "deploying" to, and we need to use copy instead of rename.
                copy=True,
                deploy_dir_path=deploy_dir_path,
            ),
            dev=self.__inputs.dev,
            loaded_data_dir_path=self.__temp_dir_path,
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
            validate_transform=False,
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
