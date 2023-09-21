import os
import tqdm
import glob
import argparse
import threading
import gzip
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import pandas as pd


def process_work(work):
    works_columns = [
        'id', 'doi', 'title', 'display_name', 'publication_year', 'publication_date', 'type', 'cited_by_count',
        'is_retracted', 'is_paratext', 'cited_by_api_url',
        # 'abstract_inverted_index' # we don't need abstract_inverted_index
    ]

    works_list = []
    primary_locations_list = []
    locations_list = []
    best_oa_location_list = []
    authorships_list = []
    biblio_list = []
    concepts_list = []
    ids_list = []
    mesh_list = []
    open_access_list = []
    referenced_works_list = []
    related_works_list = []

    work_id = work.get('id')
    works_list.append({key: value for key, value in work.items() if key in works_columns})

    primary_location = work.get('primary_location', {})
    if primary_location and primary_location.get('source') and primary_location.get('source', {}).get('id'):
        primary_locations_list.append({
            'work_id': work_id,
            'source_id': primary_location['source']['id'],
            'landing_page_url': primary_location.get('landing_page_url'),
            'pdf_url': primary_location.get('pdf_url'),
            'is_oa': primary_location.get('is_oa'),
            'version': primary_location.get('version'),
            'license': primary_location.get('license'),
        })

    # locations
    if locations := work.get('locations'):
        for location in locations:
            if location.get('source') and location.get('source').get('id'):
                locations_list.append({
                    'work_id': work_id,
                    'source_id': location['source']['id'],
                    'landing_page_url': location.get('landing_page_url'),
                    'pdf_url': location.get('pdf_url'),
                    'is_oa': location.get('is_oa'),
                    'version': location.get('version'),
                    'license': location.get('license'),
                })

    # best_oa_locations
    if best_oa_location := (work.get('best_oa_location') or {}):
        if best_oa_location.get('source') and best_oa_location.get('source').get('id'):
            best_oa_location_list.append({
                'work_id': work_id,
                'source_id': best_oa_location['source']['id'],
                'landing_page_url': best_oa_location.get('landing_page_url'),
                'pdf_url': best_oa_location.get('pdf_url'),
                'is_oa': best_oa_location.get('is_oa'),
                'version': best_oa_location.get('version'),
                'license': best_oa_location.get('license'),
            })

    # authorships
    if authorships := work.get('authorships'):
        for authorship in authorships:
            if author_id := authorship.get('author', {}).get('id'):
                institutions = authorship.get('institutions')
                institution_ids = [i.get('id') for i in institutions]
                institution_ids = [i for i in institution_ids if i]
                institution_ids = institution_ids or [None]

                for institution_id in institution_ids:
                    authorships_list.append({
                        'work_id': work_id,
                        'author_position': authorship.get('author_position'),
                        'author_id': author_id,
                        'institution_id': institution_id,
                        'raw_affiliation_string': authorship.get('raw_affiliation_string'),
                    })
    # biblio
    if biblio := work.get('biblio'):
        biblio['work_id'] = work_id
        biblio_list.append(biblio)

        # concepts
    for concept in work.get('concepts'):
        if concept_id := concept.get('id'):
            concepts_list.append({
                'work_id': work_id,
                'concept_id': concept_id,
                'score': concept.get('score'),
            })

    # ids
    if ids := work.get('ids'):
        ids['work_id'] = work_id
        ids_list.append(ids)

    # mesh
    for mesh in work.get('mesh'):
        mesh['work_id'] = work_id
        mesh_list.append(mesh)

    # open_access
    if open_access := work.get('open_access'):
        open_access['work_id'] = work_id
        open_access_list.append(open_access)

    # referenced_works
    for referenced_work in work.get('referenced_works'):
        if referenced_work:
            referenced_works_list.append({
                'work_id': work_id,
                'referenced_work_id': referenced_work
            })

    # related_works
    for related_work in work.get('related_works'):
        if related_work:
            related_works_list.append({
                'work_id': work_id,
                'related_work_id': related_work
            })

    data = [("works", works_list),
            ("primary_locations", primary_locations_list),
            ("locations", locations_list),
            ("best_oa_locations", best_oa_location_list),
            ("authorships", authorships_list),
            ("biblio", biblio_list),
            ("concepts", concepts_list),
            ("ids", ids_list),
            ("mesh", mesh_list),
            ("open_access", open_access_list),
            ("referenced_works", referenced_works_list),
            ("related_works", related_works_list)]

    return data


def process_file(num, jsonl_file_name, save_dir):
    csv_files = {
        'works': {
            'works': {
                'name': os.path.join(save_dir, f'works_{num}.csv.gz'),
                'columns': [
                    'id', 'doi', 'title', 'display_name', 'publication_year', 'publication_date', 'type',
                    'cited_by_count',
                    'is_retracted', 'is_paratext', 'cited_by_api_url',
                    # 'abstract_inverted_index' # we don't need abstract_inverted_index
                ]
            },
            'primary_locations': {
                'name': os.path.join(save_dir, f'works_primary_locations_{num}.csv.gz'),
                'columns': [
                    'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa', 'version', 'license'
                ]
            },
            'locations': {
                'name': os.path.join(save_dir, f'works_locations_{num}.csv.gz'),
                'columns': [
                    'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa', 'version', 'license'
                ]
            },
            'best_oa_locations': {
                'name': os.path.join(save_dir, f'works_best_oa_locations_{num}.csv.gz'),
                'columns': [
                    'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa', 'version', 'license'
                ]
            },
            'authorships': {
                'name': os.path.join(save_dir, f'works_authorships_{num}.csv.gz'),
                'columns': [
                    'work_id', 'author_position', 'author_id', 'institution_id', 'raw_affiliation_string'
                ]
            },
            'biblio': {
                'name': os.path.join(save_dir, f'works_biblio_{num}.csv.gz'),
                'columns': [
                    'work_id', 'volume', 'issue', 'first_page', 'last_page'
                ]
            },
            'concepts': {
                'name': os.path.join(save_dir, f'works_concepts_{num}.csv.gz'),
                'columns': [
                    'work_id', 'concept_id', 'score'
                ]
            },
            'ids': {
                'name': os.path.join(save_dir, f'works_ids_{num}.csv.gz'),
                'columns': [
                    'work_id', 'openalex', 'doi', 'mag', 'pmid', 'pmcid'
                ]
            },
            'mesh': {
                'name': os.path.join(save_dir, f'works_mesh_{num}.csv.gz'),
                'columns': [
                    'work_id', 'descriptor_ui', 'descriptor_name', 'qualifier_ui', 'qualifier_name', 'is_major_topic'
                ]
            },
            'open_access': {
                'name': os.path.join(save_dir, f'works_open_access_{num}.csv.gz'),
                'columns': [
                    'work_id', 'is_oa', 'oa_status', 'oa_url', 'any_repository_has_fulltext'
                ]
            },
            'referenced_works': {
                'name': os.path.join(save_dir, f'works_referenced_works_{num}.csv.gz'),
                'columns': [
                    'work_id', 'referenced_work_id'
                ]
            },
            'related_works': {
                'name': os.path.join(save_dir, f'works_related_works_{num}.csv.gz'),
                'columns': [
                    'work_id', 'related_work_id'
                ]
            },
        },
    }

    file_spec = csv_files['works']

    for tabel, desc in file_spec.items():
        path = desc['name']
        columns = desc['columns']
        header_df = pd.DataFrame(columns=columns)
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            header_df.to_csv(f, index=False)
    data_caches = defaultdict(list)
    buffer_size = 2000
    with gzip.open(jsonl_file_name, 'r') as works_jsonl:
        for num_of_i, work_json in tqdm.tqdm(enumerate(works_jsonl), desc=f"Processing {jsonl_file_name}"):
            if not work_json.strip():
                continue
            work = json.loads(work_json)
            processed_data = process_work(work)
            for key, values in processed_data:
                data_caches[key] += values

            if len(data_caches["works"]) >= buffer_size:
                # start = time.time()
                for key, values in data_caches.items():
                    if len(values) == 0:
                        continue
                    save_path = file_spec[key]['name']
                    df = pd.DataFrame(values)
                    df.to_csv(save_path, mode='a', index=False, header=False, compression='gzip',
                              columns=file_spec[key]['columns'])
                data_caches = defaultdict(list)
                # end = time.time()
                # print(f"Saving used:{end - start}")
    for key, values in data_caches.items():
        if len(values) == 0:
            continue
        save_path = file_spec[key]['name']
        df = pd.DataFrame(values)
        df.to_csv(save_path, mode='at', index=False, header=False, compression='gzip',
                  columns=file_spec[key]['columns'])


if __name__ == '__main__':
    MAX_CONCURRENT_THREADS = 16  # For example, limit to 4 concurrent threads

    # Create a semaphore to control the number of concurrent threads
    thread_semaphore = threading.Semaphore(MAX_CONCURRENT_THREADS)

    parser = argparse.ArgumentParser(description="flatten-openalex-works-to-csv")
    parser.add_argument("--snapshot_dir", type=str, default="./data/openalex/openalex-snapshot",
                        help="snapshot_dir")
    parser.add_argument("--csv_dir", type=str, default="./data/openalex/csv-files",
                        help="csv_dir")
    args = parser.parse_args()

    SNAPSHOT_DIR = args.snapshot_dir
    CSV_DIR = args.csv_dir
    threads = []
    all_files = sorted(glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'works', '*', '*.gz')))
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_THREADS) as executor:
        for i, jsonl_file_name in tqdm.tqdm(
                enumerate(all_files)):
            thread = executor.submit(process_file, i, jsonl_file_name, CSV_DIR)
            threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.result()
