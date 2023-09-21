# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     import_csv_to_postgresql
   Description :
   Author :       igorwang
   date：          31/8/2023
-------------------------------------------------
   Change Activity:
                   31/8/2023:
-------------------------------------------------
"""
import tqdm
import psycopg2
from psycopg2 import sql

import argparse
import glob
import os
import re
import gzip

sql_map = {
    "authors": "Copy openalex.authors (id, orcid, display_name, display_name_alternatives, works_count, cited_by_count, last_known_institution, works_api_url, updated_date) from stdin WITH CSV HEADER DELIMITER as ','",
    "authors_ids": "Copy openalex.authors_ids (author_id, openalex, orcid, scopus, twitter, wikipedia, mag) from stdin WITH CSV HEADER DELIMITER as ','",
    "authors_counts_by_year": "Copy openalex.authors_counts_by_year (author_id, year, works_count, cited_by_count,oa_works_count) from stdin WITH CSV HEADER DELIMITER as ','",
    "concepts": "Copy openalex.concepts (id, wikidata, display_name, level, description, works_count, cited_by_count, image_url, image_thumbnail_url, works_api_url, updated_date) from stdin WITH CSV HEADER DELIMITER as ','",
    "concepts_ancestors": "Copy openalex.concepts_ancestors (concept_id, ancestor_id) from stdin WITH CSV HEADER DELIMITER as ','",
    "concepts_counts_by_year": "Copy openalex.concepts_counts_by_year (concept_id, year, works_count, cited_by_count, oa_works_count) from stdin WITH CSV HEADER DELIMITER as ','",
    "concepts_ids": "Copy openalex.concepts_ids (concept_id, openalex, wikidata, wikipedia, umls_aui, umls_cui, mag) from stdin WITH CSV HEADER DELIMITER as ','",
    "concepts_related_concepts": "Copy openalex.concepts_related_concepts (concept_id, related_concept_id, score) from stdin WITH CSV HEADER DELIMITER as ','",
    "institutions": "Copy openalex.institutions (id, ror, display_name, country_code, type, homepage_url, image_url, image_thumbnail_url, display_name_acroynyms, display_name_alternatives, works_count, cited_by_count, works_api_url, updated_date) from stdin WITH CSV HEADER DELIMITER as ','",
    "institutions_ids": "Copy openalex.institutions_ids (institution_id, openalex, ror, grid, wikipedia, wikidata, mag) from stdin WITH CSV HEADER DELIMITER as ','",
    "institutions_geo": "Copy openalex.institutions_geo (institution_id, city, geonames_city_id, region, country_code, country, latitude, longitude) from stdin WITH CSV HEADER DELIMITER as ','",
    "institutions_associated_institutions": "Copy openalex.institutions_associated_institutions (institution_id, associated_institution_id, relationship) from stdin WITH CSV HEADER DELIMITER as ','",
    "institutions_counts_by_year": "Copy openalex.institutions_counts_by_year (institution_id, year, works_count, cited_by_count, oa_works_count) from stdin WITH CSV HEADER DELIMITER as ','",
    "publishers": "Copy openalex.publishers (id, display_name, alternate_titles, country_codes, hierarchy_level, parent_publisher, works_count, cited_by_count, sources_api_url, updated_date) from stdin WITH CSV HEADER DELIMITER as ','",
    "publishers_ids": "Copy openalex.publishers_ids (publisher_id, openalex, ror, wikidata) from stdin WITH CSV HEADER DELIMITER as ','",
    "publishers_counts_by_year": "Copy openalex.publishers_counts_by_year (publisher_id, year, works_count, cited_by_count, oa_works_count) from stdin WITH CSV HEADER DELIMITER as ','",
    "sources": "Copy openalex.sources (id, issn_l, issn, display_name, publisher, works_count, cited_by_count, is_oa, is_in_doaj, homepage_url, works_api_url, updated_date) from stdin WITH CSV HEADER DELIMITER as ','",
    "sources_ids": "Copy openalex.sources_ids (source_id, openalex, issn_l, issn, mag, wikidata, fatcat) from stdin WITH CSV HEADER DELIMITER as ','",
    "sources_counts_by_year": "Copy openalex.sources_counts_by_year (source_id, year, works_count, cited_by_count,oa_works_count) from stdin WITH CSV HEADER DELIMITER as ','",
    "works": "Copy openalex.works (id, doi, title, display_name, publication_year, publication_date, type, cited_by_count, is_retracted, is_paratext, cited_by_api_url) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_primary_locations": "Copy openalex.works_primary_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_locations": "Copy openalex.works_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_best_oa_locations": "Copy openalex.works_best_oa_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_authorships": "Copy openalex.works_authorships (work_id, author_position, author_id, institution_id, raw_affiliation_string) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_biblio": "Copy openalex.works_biblio (work_id, volume, issue, first_page, last_page) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_concepts": "Copy openalex.works_concepts (work_id, concept_id, score) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_ids": "Copy openalex.works_ids (work_id, openalex, doi, mag, pmid, pmcid) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_mesh": "Copy openalex.works_mesh (work_id, descriptor_ui, descriptor_name, qualifier_ui, qualifier_name, is_major_topic) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_open_access": "Copy openalex.works_open_access (work_id, is_oa, oa_status, oa_url, any_repository_has_fulltext) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_referenced_works": "Copy openalex.works_referenced_works (work_id, referenced_work_id) from stdin WITH CSV HEADER DELIMITER as ','",
    "works_related_works": "Copy openalex.works_related_works (work_id, related_work_id) from stdin WITH CSV HEADER DELIMITER as ','",
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="import_csv_to_postgresql")
    parser.add_argument("--csv_dir", type=str, default="./data/openalex/csv-files",
                        help="csv_dir")
    args = parser.parse_args()

    # change your db connexion config
    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="PASSWORD",
        host="127.0.0.1",
        port="45432")

    csv_dir = args.csv_dir
    files = sorted(glob.glob(os.path.join(csv_dir, '*.gz')))
    for fp in tqdm.tqdm(files):
        _, filename = os.path.split(fp)
        key = filename.replace(".csv.gz", "")
        key = re.sub('_\d*$', "", key)
        # 解析文件名
        # 创建一个游标对象
        cur = conn.cursor()
        sql = sql_map.get(key, "")
        # if 'works_open_access' not in sql: # use this for import specific table
        #     continue
        sql = sql.format(file_path=fp)
        # 定义 COPY 命令的 SQL 查询
        print(sql)
        try:
            with gzip.open(fp, 'rt') as f:
                cur.copy_expert(sql=sql, file=f)
            conn.commit()
        except Exception as e:
            print("发生异常：", e)
            # 执行回滚操作，确保事务状态不会被标记为 "aborted"
            conn.rollback()
            continue
        cur.close()
    conn.close()
