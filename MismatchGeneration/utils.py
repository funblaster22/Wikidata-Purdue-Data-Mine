"""
Utility functions for accessing Wikidata's data and checking Mismatch Finder submissions.

Contents:
    download_wikidata_json_dump,
    _get_entity_value,
    _process_json_entry,
    parse_wikidata_dump_to_ndjson,
    _validate_url,
    check_mf_formatting
"""

import bz2
import json
import logging
import os
import requests
from urllib.parse import urlparse
import warnings

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

logging.disable(logging.WARNING)
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"


def download_wikidata_json_dump(target_dir="Data", dump_id=False):
    """
    Downloads the most recent stable dump of Wikidata if it is not already in the specified directory.

    Parameters
    ----------
        target_dir : str (default=Data)
            The directory in the pwd into which files should be downloaded.

        dump_id : str (default=False)
            The id of an explicit Wikidata dump that the user wants to download.

    Returns
    -------
        A downloaded bz2 compressed Wikidata dump with printed information on the downloaded file.
    """
    import tensorflow as tf

    if not os.path.exists(target_dir):
        print(f"Making {target_dir} directory")
        os.makedirs(target_dir)

    # Find all files to download.
    base_url = "https://dumps.wikimedia.org/other/wikibase/wikidatawiki"
    dumps_index = requests.get(base_url).text
    dumps_soup_index = BeautifulSoup(dumps_index, "html.parser")

    all_dumps = [
        a["href"].split("/")[0]
        for a in dumps_soup_index.find_all("a")
        if a.has_attr("href")
    ]

    # Derive URL for the dump to download.
    if not dump_id:
        target_dump_id = "latest-all.json.bz2"
        target_local_file_path = f"{target_dir}/{target_dump_id}"
        target_dump_url = f"{base_url}/{target_dump_id}"

    else:
        if dump_id in all_dumps:
            target_dump_dir_url = f"{base_url}/{dump_id}"
            target_dump_dir_index = requests.get(target_dump_dir_url).text
            target_dump_dir_soup_index = BeautifulSoup(
                target_dump_dir_index, "html.parser"
            )

            all_dump_files = [
                a["href"].split("/")[0]
                for a in target_dump_dir_soup_index.find_all("a")
                if a.has_attr("href")
            ]

            target_dump_id = f"wikidata-{dump_id}-all.json.bz2"
            if target_dump_id in all_dump_files:
                target_local_file_path = f"{target_dir}/{target_dump_id}"
                target_dump_url = f"{target_dump_dir_url}/{target_dump_id}"

            else:
                raise ValueError(
                    "The passed value for `dump_id` does not have a bz2 compressed dump for all of Wikidata."
                )

        else:
            raise ValueError(
                "The passed value for `dump_id` is not a valid Wikidata dump."
            )

    print(f"Target Wikidata dump file is '{target_dump_id}'.\n")

    # Check if the dump has already been downloaded.
    if os.path.exists(target_local_file_path):
        file_size = os.stat(target_local_file_path).st_size / 1e9
        print(
            f"The desired dump already exists locally at {target_local_file_path} ({round(file_size, 2):,} GBs). Skipping download."
        )

    else:
        print(
            f"The desired dump does not exist locally. Starting download to {target_local_file_path}..."
        )

        cache_subdir = target_dir.split("/")[-1]
        cache_dir = "/".join(target_dir.split("/")[:-1])
        if cache_dir == "":
            cache_subdir = target_dir
            cache_dir = "."

        # Use Tensorflow for the download so we download iteratively and have a progress bar.
        saved_file_path = tf.keras.utils.get_file(
            fname=target_dump_id,
            origin=target_dump_url,
            extract=True,
            archive_format="auto",
            cache_subdir=cache_subdir,
            cache_dir=cache_dir,
        )

        file_size = os.stat(target_local_file_path).st_size / 1e9
        print(
            f"Downloaded a compressed dump of Wikidata QIDs ({round(file_size, 2):,} GBs)."
        )


def _get_claims_embedded_value(
    claims_index: dict, data_type: str, value_prop: str = None
):
    """
    Returns the embedded value of a given entity given its data type.

    Parameters
    ----------
        claims_index : dict
            An element of the claims for a Wikidata item's properties.

        data_type: str
            The type of data that the Wikidata item is.

        value_prop : str (default=None)
            Property of the value like a unit or language if needed.

    Returns
    -------
        The string value that's either the value itself or one of the value's keys.
    """
    # Please note that this doesn't account for all data types including coordinates and others.
    data_value = claims_index["mainsnak"]["datavalue"]["value"]
    if data_type == "wikibase-entityid":
        return data_value["id"]

    elif data_type == "quantity":
        if value_prop == None:
            return data_value["amount"]

        else:
            if value_prop == data_value["unit"]:
                return data_value["amount"]

            else:
                return data_value

    elif data_type == "monolingualtext":
        if value_prop == None:
            return data_value["text"]

        else:
            if value_prop == data_value["language"]:
                return data_value["text"]

            else:
                return data_value

    else:
        return data_value


def _process_json_entry(
    entry: str,
    pids: list = None,
    pid_values: list = None,
    pid_value_props: list = None,
    prop_intersection: bool = True,
):
    """
    Checks properties and property values in a Wikidata item during compressed dump parsing.

    Parameters
    ----------
        entry : str
            The dump entry to conditionally check whether it should be included.

        pids : list(str) (default=None)
            The property ids that should be subset by.

            Data types can be wikibase-entityid, quantity, monolingualtext or types without embedded values.

        pid_values : list(str) (default=None)
            The values of the properties to subset by.

        pid_value_props : str or list(str)
            Properties of the values like a unit or language if needed.

        prop_intersection : bool (default=True)
            Whether all pids and pid_values need to be present for the element to be included.

    Returns
    -------
        A boolean on whether the entry meets the criteria of the properties and values.
    """
    # If no PIDs are passed then all entries are included in the output.
    if pids is None:
        return True

    try:
        # Check that all PIDS are present.
        if prop_intersection:
            prop_claims = {p: entry["claims"][p] for p in pids}

        else:
            prop_claims = {}
            for p in pids:
                if p in entry["claims"].keys():
                    prop_claims[p] = entry["claims"][p]

            if len(prop_claims) == 0:
                return False

        if pid_values is None:
            return True

        # For PID values check that each is included in what Wikidata has.
        for i, pid in enumerate(pids):
            # Derive the data type for the given property value.
            # There always is only one data type, but not all claim elements have it.
            data_type = None
            for claim in prop_claims[pid]:
                try:
                    data_type = claim["mainsnak"]["datavalue"]["type"]
                    if data_type != None:
                        break
                except:
                    pass

            if pid_value_props == None:
                all_prop_values = [
                    _get_claims_embedded_value(
                        claims_index=prop_claims[pid][idx],
                        data_type=data_type,
                        value_prop=None,
                    )
                    for idx in range(len(prop_claims[pid]))
                ]

            else:
                all_prop_values = [
                    _get_claims_embedded_value(
                        claims_index=prop_claims[pid][idx],
                        data_type=data_type,
                        value_prop=pid_value_props[i],
                    )
                    for idx in range(len(prop_claims[pid]))
                ]

            assert pid_values[i] in all_prop_values
            return True

    except Exception as e:
        return False


def parse_wikidata_dump_to_ndjson(
    pids: str | list = None,
    pid_values: str | list = None,
    pid_value_props: str | list = None,
    prop_intersection: bool = True,
    output_file_path: str = "Data/parsed-dump.ndjson",
    input_file_path: str = "Data/latest-all.json.bz2",
    output_limit: int = None,
    input_limit: int = None,
    verbose: bool = True,
):
    """
    Parse a bz2 Wikidata dump for specific entries based on properties.

    Parameters
    ----------
        pids : str or list(str) (default=None)
            The property ids that should be subset by.

        pid_values : str or list(str) (default=None)
            The values of the properties to subset by.

        pid_value_props : str or list(str) (default=None)
            Properties of the values like a unit or language if needed.

        prop_intersection : bool (default=True)
            Whether all pids and pid_values need to be present for the element to be included.

        output_file_path : str (default=Data/parsed-dump.ndjson)
            The name of the final output ndjson file.

        input_file_path : str (default=Data/latest-all.json.bz2)
            The path to the directory where the data is stored.

        output_limit : int (default=None)
            An optional limit of the number of entities to find.

        input_limit : int (default=None)
            An optional limit of the number of entities to search through.

        verbose : bool (default=True)
            Whether to show a tqdm progress bar for the process.
    """
    if pids is None:
        print(
            "You have not provided a value to the `pids` argument. All entries will be returned.\n"
        )

    if pid_values:
        assert (
            len(pids) == len(pid_values)
        ), "If providing a value for `pid_values`, then one value should be provided for each `pid`."

    if pid_value_props:
        assert (
            len(pids) == len(pid_value_props)
        ), "If providing a value for `pid_value_props`, then one value should be provided for each `pid` (pass None if not needed)."

    pids = [pids] if isinstance(pids, str) else pids
    pid_values = [pid_values] if isinstance(pid_values, str) else pid_values
    pid_value_props = (
        [pid_value_props] if isinstance(pid_value_props, str) else pid_value_props
    )

    rewrite_file = False
    if not os.path.exists(output_file_path):
        print(f"Making {output_file_path} file for the output.")
        open(output_file_path, "a").close()

    else:
        print(f"The output file {output_file_path} already exists.")
        print("This file will be rewritten.")
        rewrite_file = True

    if input_limit is not None:
        pbar_limit = input_limit
        pbar_desc = "Entries processed"

    elif output_limit is not None:
        pbar_limit = output_limit
        pbar_desc = "Outputs returned"

    else:
        pbar_limit = None
        pbar_desc = "Entries processed"

    lines_read = 0
    entities_returned = 0
    with bz2.BZ2File(input_file_path, "r") as f_in:
        if rewrite_file:
            with open(output_file_path, "r") as f_out:
                _ = f_out.read()

        with open(output_file_path, "w") as f_out:
            pbar = tqdm(
                total=pbar_limit,
                desc=pbar_desc,
                unit="entries",
                disable=not verbose,
            )
            for line in f_in:
                # Skip the first value as it's a new line character.
                if lines_read == 0:
                    lines_read += 1
                    pbar.update()
                    pass

                else:
                    try:
                        # Read in to the last two characters that are a comma and new line.
                        json_entry = json.loads(line.decode("utf-8")[:-2])
                        if _process_json_entry(
                            entry=json_entry,
                            pids=pids,
                            pid_values=pid_values,
                            pid_value_props=pid_value_props,
                            prop_intersection=prop_intersection,
                        ):
                            f_out.write(json.dumps(json_entry) + "\n")

                            entities_returned += 1
                            if output_limit:
                                pbar.update()

                            if entities_returned == output_limit:
                                lines_read += 1
                                break

                    except:
                        try:
                            # The last element of the dump doesn't have a comma after it, just a new line.
                            json_entry = json.loads(line.decode("utf-8")[:-1])
                            if _process_json_entry(
                                entry=json_entry,
                                pids=pids,
                                pid_values=pid_values,
                                pid_value_props=pid_value_props,
                                prop_intersection=prop_intersection,
                            ):
                                f_out.write(json.dumps(json_entry) + "\n")

                                entities_returned += 1
                                if output_limit:
                                    pbar.update()

                                if entities_returned == output_limit:
                                    lines_read += 1
                                    break

                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON at entry {lines_read + 1}: {e}")

                    lines_read += 1
                    if input_limit:
                        pbar.update()

                    if lines_read == input_limit:
                        break

    print(
        f"Parsed {lines_read:,} entries in the JSON dump into an NDJSON file with {entities_returned:,} entities."
    )


def _validate_url(url):
    """
    Check that a value is not null and is a valid URL if so.
    """
    if pd.isnull(url) is None:
        try:
            url_parse = urlparse(url)
            return all([url_parse.scheme, url_parse.netloc])

        except:
            return False

    return True


def check_mf_formatting(df: pd.DataFrame):
    """
    Checks a Pandas DataFrame to see whether it will produce a valid CSV for Mismatch Finder.

    For conditions, please see:
    https://github.com/wmde/wikidata-mismatch-finder/blob/main/docs/UserGuide.md#creating-a-mismatches-import-file

    Parameters
    ----------
        df: pandas.DataFrame
            A DataFrame for which we want to run df.to_csv().

    Returns
    -------
        A message of whether or not the DataFrame is valid and directions to fix any issues if needed.
    """
    df_formatted_correctly = True
    correction_instruction = []

    # 1. Check that all required columns are included.
    required_columns = [
        "item_id",
        "statement_guid",
        "property_id",
        "wikidata_value",
        "meta_wikidata_value",
        "external_value",
        "external_url",
        "type",
    ]

    if list(df.columns) != required_columns:
        df_formatted_correctly = False
        required_columns_string = "'" + "', '".join(required_columns) + "'"
        correction_instruction.append(
            f"Please check that the following columns are present in this exact order:\n    {required_columns_string}"
        )

    # 2. Check that all QIDs and PIDs are formatted correctly.
    id_columns = ["item_id", "property_id"]
    id_columns_included = [c for c in id_columns if c in df.columns]
    columns_with_invalid_ids = []
    for c in id_columns_included:
        if c == "item_id":
            if df[c].astype(str).str.match(r"^Q\d+$").all() != True:
                columns_with_invalid_ids.append(c)

        elif c == "property_id":
            if df[c].astype(str).str.match(r"^P\d+$").all() != True:
                columns_with_invalid_ids.append(c)

    if columns_with_invalid_ids:
        df_formatted_correctly = False
        invalid_id_correction_message = (
            "Please assure that the following columns have valid ids:"
        )
        for c in columns_with_invalid_ids:
            invalid_id_correction_message += f"\n    - {c}"

        correction_instruction.append(invalid_id_correction_message)

    # 3. Check that there are no nulls in non-optional columns.
    required_value_columns = ["item_id", "property_id", "external_value"]
    required_value_columns_included = [
        c for c in required_value_columns if c in df.columns
    ]
    columns_with_nulls = []
    for c in required_value_columns_included:
        if df[c].isnull().values.any() == True:
            columns_with_nulls.append(c)

    if columns_with_nulls:
        df_formatted_correctly = False
        null_value_correction_message = (
            "Please assure that the following columns do not have null values:"
        )
        for c in columns_with_nulls:
            null_value_correction_message += f"\n    - {c}"

        correction_instruction.append(null_value_correction_message)

    # 4. Check that values exist for all rows where there is a statement.
    if "statement_guid" in df.columns and "wikidata_value" in df.columns:
        guids = df["statement_guid"].values
        wd_values = df["wikidata_value"].values

        check_empty_value_list = [
            not pd.isnull(wd_values[i]) and pd.isnull(guids[i])
            for i in range(len(guids))
        ]

        if True in check_empty_value_list:
            df_formatted_correctly = False
            correction_instruction.append(
                "Please assure that `statement_guid` is null only in cases where `wikidata_value` is as well."
            )

    # 5. Check that all external URLs are valid.
    if "external_url" in df.columns:
        url_validation_checks = [_validate_url(u) for u in df["external_url"]]
        if False in url_validation_checks:
            df_formatted_correctly = False
            invalid_urls = [
                df["external_url"][i]
                for i in range(len(url_validation_checks))
                if url_validation_checks[i] == False
            ]
            url_correction_message = "Please check the following URLs in `external_url` to make sure that they're valid:"
            for u in invalid_urls:
                url_correction_message += f"\n    - {u}"

            correction_instruction.append(url_correction_message)

    # 6. Check that all type values are 'statement', 'qualifier' or a null value that will be made 'statement'.
    if "type" in df.columns:
        allowed_types = set(["statement", "qualifier", np.nan])
        included_types = set(df["type"].unique())
        if not set(included_types).issubset(allowed_types):
            df_formatted_correctly = False
            correction_instruction.append(
                "Please check that the `type` column contains only: 'statement', 'qualifier' or a null value."
            )

    # 7. Check that values for certain columns are less than 1,500 characters.
    check_value_length_columns = ["wikidata_value", "external_value", "external_url"]
    check_value_length_columns_included = [
        c for c in check_value_length_columns if c in df.columns
    ]
    columns_with_too_long_values = []
    for c in check_value_length_columns_included:
        if (df[c].str.len() > 1500).any() == True:
            columns_with_too_long_values.append(c)

    if columns_with_too_long_values:
        df_formatted_correctly = False
        too_long_value_correction_message = "Please assure that the following columns do not have values over 1,500 characters:"
        for c in columns_with_too_long_values:
            too_long_value_correction_message += f"\n    - {c}"

        correction_instruction.append(too_long_value_correction_message)

    # Raise exception if there's a data formatting issue or print that all checks have passed.
    if df_formatted_correctly == False:
        mf_file_creation_directions = """
There's a problem with the DataFrame. Please see the Mismatch Finder file creation directions on GitHub:

https://github.com/wmde/wikidata-mismatch-finder/blob/main/docs/UserGuide.md#creating-a-mismatches-import-file

Directions on how to fix the DataFrame are also detailed below:
"""
        value_error_message = mf_file_creation_directions + "".join(
            f"\n{i+1}. {correction_instruction[i]}\n"
            for i in range(len(correction_instruction))
        )
        raise ValueError(value_error_message)

    else:
        print(
            "All checks have passed! The data is ready to be uploaded to Mismatch Finder."
        )
