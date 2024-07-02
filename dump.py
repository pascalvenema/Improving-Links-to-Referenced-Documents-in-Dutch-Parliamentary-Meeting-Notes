from daily import extract_links_from_minute
from helpers import list_file_paths, retrieve_publications
import pandas as pd
import numpy as np
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

count = 0


def generate_dump(
    woogle_dump_path: str,
    output_folder: str,
    number_of_chunks: int = 50,
    number_of_threads: int = 7,
):

    def dumper(chunk, chunk_index, total_rows):
        df_dump = pd.DataFrame(
            columns=[
                "foi_dossierId",
                "dc_externalIdentifier",
                "dc_itemUrlHtml",
                "available",
                "ref",
                "title",
                "dossiertitel",
                "countOriginal",
                "countDetectedExplicit",
                "countDetectedLocalAlias",
            ]
        )

        for index, row in chunk.iterrows():
            global count
            count += 1
            if count % 10 == 0:
                progress_percentage = (count / total_rows) * 100
                print(f"{count} / {total_rows} ({progress_percentage:.2f}%)")
            handeling_id_officielebekendmakingen = row["dc_externalIdentifier"]
            handeling_id_woogle = row["foi_dossierId"]
            minute = None
            xml_url = ""
            available = ""
            # Check if the XML of the Handeling can be retrieved
            publications = retrieve_publications(
                query_list=["dt.identifier == " + handeling_id_officielebekendmakingen]
            )
            if len(publications) == 0:
                print(
                    "No publications found for the given minute_id: "
                    + handeling_id_officielebekendmakingen
                )
            else:
                minute = publications[0]
                xml_url = next((d["xml"] for d in minute.itemUrl if "xml" in d), None)
                available = minute.available[0]

            tuple_existing_refs, tuple_explicit_refs, tuple_local_alias_refs = (
                extract_links_from_minute(
                    xml_url=xml_url, existing_references=minute.behandeldDossier
                )
            )

            combined_tuples = tuple_existing_refs.copy()
            combined_tuples.extend(tuple_explicit_refs)
            combined_tuples.extend(tuple_local_alias_refs)

            count_original = Counter([key for key, value in tuple_existing_refs])
            count_detected_explicit = Counter(
                [key for key, value in tuple_explicit_refs]
            )
            count_detected_local_alias = Counter(
                [key for key, value in tuple_local_alias_refs]
            )

            all_unique_refs = set(
                [key for key, value in tuple_existing_refs]
                + [key for key, value in tuple_explicit_refs]
                + [key for key, value in tuple_local_alias_refs]
            )

            rows_list = []

            if len(all_unique_refs) > 0:
                for ref in all_unique_refs:
                    documenttitel = None
                    dossiertitel = None
                    resource = None
                    html_url = None
                    for key, value in combined_tuples:
                        if key == ref:
                            resource = value
                            break
                    if resource is None:
                        raise KeyError("Resource not found")
                    dossier = False
                    if isinstance(resource, list):  # Kamerdossier
                        resource = resource[0]
                        dossiertitel = resource.dossiertitel[0]
                        html_url = next(
                            (d["html"] for d in resource.itemUrl if "html" in d), None
                        )
                    else:  # Kamerstuk
                        dossiertitel = resource.dossiertitel[0]
                        documenttitel = resource.documenttitel[0]
                        html_url = next(
                            (d["html"] for d in resource.itemUrl if "html" in d), None
                        )

                    row = {
                        "foi_dossierId": str(handeling_id_woogle),
                        "dc_externalIdentifier": str(
                            handeling_id_officielebekendmakingen
                        ),
                        "dc_itemUrlHtml": str(html_url),
                        "available": str(available),
                        "ref": str(ref),
                        "title": str(documenttitel),
                        "dossiertitel": str(dossiertitel),
                        "countOriginal": count_original.get(ref, 0),
                        "countDetectedExplicit": count_detected_explicit.get(ref, 0),
                        "countDetectedLocalAlias": count_detected_local_alias.get(
                            ref, 0
                        ),
                    }

                    rows_list.append(row)

                new_rows_df = pd.DataFrame(rows_list)
                df_dump = pd.concat([df_dump, new_rows_df], ignore_index=True)
            else:
                continue

        # Save intermediate chunk results to CSV
        chunk_filename = output_folder + "chunks/" + f"dump_chunk_{chunk_index}.csv"
        df_dump.to_csv(chunk_filename, index=False)
        print(f"Chunk {chunk_index} saved")
        return chunk_filename

    # Location of the most recent dump of minutes from Woogle
    df = pd.read_csv(woogle_dump_path)

    # Take only the identifiers
    df_identifiers = df[["dc_externalIdentifier", "foi_dossierId"]]

    # FOR DEBUGGING
    df_identifiers = df_identifiers.sample(20)

    df_chunks = np.array_split(df_identifiers, number_of_chunks)

    with ThreadPoolExecutor(max_workers=number_of_threads) as executor:
        futures = [
            executor.submit(dumper, chunk, i, (len(df_identifiers)))
            for i, chunk in enumerate(df_chunks)
        ]

        chunk_files = [future.result() for future in futures]

    chunks_path = output_folder + "chunks/"
    chunk_csvs = list_file_paths(chunks_path)

    # Combine all chunks into one DataFrame
    df_final_dump = pd.concat(
        [
            pd.read_csv(file)
            for file in chunk_csvs
            if file != (chunks_path + ".DS_Store")
        ],
        ignore_index=True,
    )

    # Save the final dataframe to a csv file
    final_filename = output_folder + "dump_" + str(datetime.now()) + ".csv"
    df_final_dump.to_csv(final_filename, index=False)


generate_dump(
    woogle_dump_path="/Users/pascalvenema/Documents/GitHub/bachelors-thesis/Notebooks/Pipeline/docs/woo_handelingen_excl_bodytext.csv",
    output_folder="/Volumes/Samsung Portable SSD T5 Media/refactor/",
    number_of_chunks=2,
    number_of_threads=2,
)
