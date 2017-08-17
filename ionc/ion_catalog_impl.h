/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

#ifndef ION_CATALOG_IMPL_H_
#define ION_CATALOG_IMPL_H_

#ifdef __cplusplus
extern "C" {
#endif

struct _ion_catalog
{
    void                *owner;
    ION_SYMBOL_TABLE    *system_symbol_table;
    ION_COLLECTION       table_list;    // collection of ION_SYMBOL_TABLE *

};

// internal (pointer based helpers) functions for catalog (in ion_catalog.c)
iERR _ion_catalog_open_with_owner_helper(ION_CATALOG **p_pcatalog, hOWNER owner);
iERR _ion_catalog_get_symbol_table_count_helper(ION_CATALOG *pcatalog, int32_t *p_count);
iERR _ion_catalog_add_symbol_table_helper(ION_CATALOG *pcatalog, ION_SYMBOL_TABLE *psymtab);
iERR _ion_catalog_find_symbol_table_helper(ION_CATALOG *pcatalog, ION_STRING *name, int32_t version, ION_SYMBOL_TABLE **p_psymtab);
iERR _ion_catalog_find_best_match_helper(ION_CATALOG *pcatalog, ION_STRING *name, int32_t version, int32_t max_id, ION_SYMBOL_TABLE **p_psymtab);
iERR _ion_catalog_release_symbol_table_helper(ION_CATALOG *pcatalog, ION_SYMBOL_TABLE *psymtab);
iERR _ion_catalog_close_helper(ION_CATALOG *pcatalog);

#ifdef __cplusplus
}
#endif

#endif /* ION_CATALOG_IMPL_H_ */
