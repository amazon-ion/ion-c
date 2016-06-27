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

#ifndef ION_CATALOG_H_
#define ION_CATALOG_H_

#include "ion_types.h"
#include "ion_platform_config.h"

#ifdef __cplusplus
extern "C" {
#endif

// Ion Symbol Catalog implementation
//
ION_API_EXPORT iERR ion_catalog_open                      (hCATALOG *p_hcatalog);
ION_API_EXPORT iERR ion_catalog_open_with_owner           (hCATALOG *p_hcatalog, hOWNER owner);
ION_API_EXPORT iERR ion_catalog_get_symbol_table_count    (hCATALOG hcatalog, int32_t *p_count);
ION_API_EXPORT iERR ion_catalog_add_symbol_table          (hCATALOG hcatalog, hSYMTAB symtab);
ION_API_EXPORT iERR ion_catalog_find_symbol_table         (hCATALOG hcatalog, iSTRING name, long version, hSYMTAB *p_symtab);
ION_API_EXPORT iERR ion_catalog_find_best_match           (hCATALOG hcatalog, iSTRING name, long version, hSYMTAB *p_symtab); // or newest version of a symtab pass in version == 0
ION_API_EXPORT iERR ion_catalog_release_symbol_table      (hCATALOG hcatalog, hSYMTAB symtab);
ION_API_EXPORT iERR ion_catalog_close                     (hCATALOG hcatalog);

#ifdef __cplusplus
}
#endif

#endif /* ION_CATALOG_H_ */
