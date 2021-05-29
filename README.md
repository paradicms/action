# Paradicms Extract-Transform-Load (ETL) Docker action

This action executes a Paradicms ETL pipeline.

## Inputs

### `data-folder`

**Required** Path to the `data` folder to use for in the pipeline. Default `data`.

<!-- ## Outputs

### `time`

The time we greeted you. -->

## Example usage

uses: paradicms/etl-action@v1
with:
  data-folder: data
