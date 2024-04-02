"""
The process of extracting Mix'n'match and Wikidata mismatches.
"""

import json
import sys
import urllib

import numpy as np
import pandas as pd
from tqdm import tqdm

PATH_TO_UTILS = "../"  # change based on your directory structure
sys.path.append(PATH_TO_UTILS)

from utils import check_mf_formatting

mnm_mismatch_request_url = (
    "https://mix-n-match.toolforge.org/api.php?query=all_issues&mode=time_mismatch"
)
with urllib.request.urlopen(mnm_mismatch_request_url) as url:
    mnm_mismatch_data = json.load(url)
mnm_mismatch_data_expanded = []
for d in mnm_mismatch_data["data"]:
    d["source"] = f"https://mix-n-match.toolforge.org/#/entry/{d['entry_id']}"
    d.pop("issue_id", None)
    d["time_mismatch"]["pid"] = d["time_mismatch"].pop("prop")
    d["time_mismatch"]["qid"] = d["time_mismatch"].pop("q")
    d["item_id"] = d["time_mismatch"]["qid"]

    mnm_mismatch_data_expanded.append(d)
print(f"{len(mnm_mismatch_data['data']):,}")
mnm_mismatch_data["data"][:2]
mnm_mismatch_data_expanded[:2]
mnm_mismatch_data_expanded = list(
    filter(
        lambda d: d["time_mismatch"]["wd_time"] != d["time_mismatch"]["mnm_time"],
        mnm_mismatch_data_expanded,
    )
)
len(mnm_mismatch_data_expanded)

# REGION
try:
    acc = pd.read_csv("mismatches.csv").to_dict(orient="records")

except:
    acc = []
blank_entry = {"id": np.NAN, "value": {"type": "value", "content": {"time": np.NAN}}}
i = -1
try:
    for entry in tqdm(mnm_mismatch_data_expanded[len(acc) :]):
        i += 1
        data = entry["time_mismatch"]
        req = f"https://www.wikidata.org/w/rest.php/wikibase/v0/entities/items/{entry['item_id']}?_fields=statements"
        try:
            with urllib.request.urlopen(req) as url:
                wd_props = json.load(url)["statements"]
        except urllib.request.HTTPError as e:
            # Fixed in newer version https://stackoverflow.com/questions/67723860/python-urllib-request-urlopen-http-error-308-permanent-redirect.
            print("Skipped", req)
            print(e)
            continue

        with urllib.request.urlopen(
            f"https://mix-n-match.toolforge.org/api.php?query=get_entry&entry={entry['entry_id']}"
        ) as url:
            try:
                ext_url = json.load(url)["data"]["entries"][entry["entry_id"]][
                    "ext_url"
                ]
            except TypeError:
                # Sometimes API can return json.load(url)["data"]["entries"] == [].
                print("Skipping malformatted external URL:", entry["source"], "idx", i)
                continue

        nonnull_wd_vals = (
            wd_props[data["pid"]] if data["pid"] in wd_props else [blank_entry]
        )
        # Sometimes, wikidata has multiple incorrect values, so fix them all.
        for wd_val in nonnull_wd_vals:
            guid = wd_val["id"]

            # Eg: Q62900754 has a death date range, which doesn't play nice, so ignore it.
            if wd_val["value"]["type"] != "value":
                print(
                    f"Skipping GUID {guid} on {entry['item_id']} {data['pid']} because it doesn't have a concrete value"
                )
                continue

            wikidata_value = wd_val["value"]["content"]["time"]

            # Isn't actually a mismatch.
            if wikidata_value == data["mnm_time"]:
                continue

            acc.append(
                {
                    "item_id": entry["item_id"],
                    "statement_guid": guid,
                    "property_id": data["pid"],
                    "wikidata_value": wikidata_value,
                    "meta_wikidata_value": np.NAN,
                    "external_value": data["mnm_time"],
                    "external_url": ext_url,
                    "type": "statement",
                }
            )

finally:
    mismatchDF = pd.DataFrame(acc)
    mismatchDF.to_csv("mismatches.csv", index=False)
    check_mf_formatting(mismatchDF)
