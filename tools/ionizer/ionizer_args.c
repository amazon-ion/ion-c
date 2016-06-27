/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

//
//  command line program to manipulate ion files
//

#include "ionizer.h"
#include "options.h"
#ifdef WIN
#include "io.h"
#endif
#include  <assert.h>

//
// local functions that are called to handle the various
// command line arguments as they are encountered
//
BOOL set_name(OC *pcur);
BOOL set_symbol_table(OC *pcur);
BOOL set_catalog_name(OC *pcur);
BOOL set_version(OC *pcur);
BOOL set_pagesize(OC *pcur);
BOOL set_write_binary(OC *pcur);
BOOL set_write_symtab(OC *pcur);
BOOL set_ugly(OC *pcur);
BOOL set_pretty(OC *pcur);
BOOL set_ascii_only(OC *pcur);
BOOL set_flush_each(OC *pcur);
BOOL set_include_counts(OC *pcur);
BOOL set_include_type_counts(OC *pcur);
BOOL set_output(OC *pcur);
BOOL set_debug(OC *pcur);
BOOL set_trace(OC *pcur);
BOOL set_help(OC *pcur);

OPT_DEF inionizer_options[] = {
    //short_name, long_name, type, hidden, required, fn, help
    { ot_none,   'a', "ascii",        FALSE, FALSE, set_ascii_only,     "sets output to force pure ascii output" },
    { ot_none,   'b', NULL,           FALSE, FALSE, set_write_binary,   "sets the output type to binary" },
    { ot_string, 'c', "catalog",      FALSE, FALSE, set_catalog_name,   "specify a catalog file (with shared symbol tables)"},
    { ot_none,   'd', "debug",         TRUE, FALSE, set_debug,          "turns on debug options" },
    { ot_int,    't', "trace",         TRUE, FALSE, set_trace,          "turns on trace options 1:args,2:parse state,3:parse fns,4:tokens,5:calls,6:time" },
    { ot_string, 'n', "name",         FALSE, FALSE, set_name,           "sets the output symbol table name" },
    { ot_string, 'o', "output",       FALSE, FALSE, set_output,         "sets the output format: ugly, binary, pretty, counts, none(scan only), types(counts)" },
    { ot_int,    'p', "pagesize",      TRUE, FALSE, set_pagesize,       "set the page size of the memory pool" },
    { ot_string, 's', "symbol_table", FALSE, FALSE, set_symbol_table,   "symbol table file for the writer (uses newest version)" },
    { ot_none,   'u', NULL,           FALSE, FALSE, set_ugly,           "sets the output format to ugly" },
    { ot_int,    'v', "version",      FALSE, FALSE, set_version,        "set the output symbol tables version" },
    { ot_none,   'f', "flush",        FALSE, FALSE, set_flush_each,     "set aggressive flushing, for binary between values, for text between lines" },
    { ot_none,   'y', NULL,           FALSE, FALSE, set_write_symtab,   "sets the output type to symbol table" },
    { ot_none,   '?', NULL,           FALSE, FALSE, set_help,           "prints this helpful message" },
    { ot_none,   'h', "help",         FALSE, FALSE, set_help,           NULL }
};
int inionizer_options_count = sizeof(inionizer_options) / sizeof(inionizer_options[0]);

// bonus fns for setting flags for debugging (aka HACK)
//extern void _ion_parser_set_debug_flag_states(BOOL is_on);
//extern void _ion_parser_set_debug_flag_actions(BOOL is_on);
//extern void _ion_parser_set_debug_flag_steps(BOOL is_on);
//extern void _ion_parser_set_debug_flag_tokens(BOOL is_on);
//extern BOOL g_ion_debug_tracing;


void ionizer_process_args(int argc, char **argv, char **p_non_argv, int *p_non_argc)
{
    BOOL  is_ok = opt_process_return_non_args(inionizer_options, inionizer_options_count, argc, argv, p_non_argv, p_non_argc);
    if (g_ionizer_print_help || !is_ok) {
        ionizer_print_help();
    }
    return;
}

void ionizer_print_help(void) 
{
    opt_help(inionizer_options
        ,inionizer_options_count
        ,"ionizer"
        , 1
        ,"Ionizer - Ion conversion tool - CSuver"
        ,"Copyright (c) 2009-2014 Amazon.com"
    );
    g_ionizer_print_help = TRUE;
}

BOOL set_name(OC *pcur) {
    char * val = opt_get_arg(pcur);
    if (!val) return FALSE;
    g_ionizer_output_symtab_name = val;
    return TRUE;
}
BOOL set_symbol_table(OC *pcur) {
    char * val = opt_get_arg(pcur);
    if (!val) return FALSE;
    g_ionizer_writer_symtab = val;
    return TRUE;
}
BOOL set_catalog_name(OC *pcur) {
    IONIZER_STR_NODE *curr, *prev, *cat_node = NULL;
    char             *val = opt_get_arg(pcur);

    if (!val) return FALSE;
    cat_node = (IONIZER_STR_NODE *)malloc(sizeof(*cat_node));
    if (!cat_node) {
        fprintf(stderr, "ERROR - out of memory allocating catalog name linked list nodes!\n");
        exit(1);
    }
    // fill out the node and 
    cat_node->str  = val;
    cat_node->next = NULL;
    for (prev=NULL,curr=g_ionizer_catalogs; curr; prev=curr, curr=curr->next)
    {
        /* the loop assignment does everything we need there are */
        /* faster ways to do this, but, really, how many catalog */
        /* files will there ever be on the command line? */
    }
    if (prev) {
        // here's the end of the list
        prev->next = cat_node;
    }
    else {
        // the easy case - this is the first node;
        g_ionizer_catalogs = cat_node;
    }
    return TRUE;
}
BOOL set_version(OC *pcur) {
    char * val = opt_get_arg(pcur);
    if (!val) return FALSE;
    g_ionizer_symtab_version = atoi(val);
    return TRUE;
}
BOOL set_pagesize(OC *pcur) {
    char * val = opt_get_arg(pcur);
    if (!val) return FALSE;
    g_ionizer_pool_page_size = atoi(val);
    return TRUE;
}

BOOL set_write_binary(OC *pcur) {
    g_ionizer_write_binary = TRUE;
    return TRUE;
}
BOOL set_write_symtab(OC *pcur) {
    g_ionizer_write_symtab = TRUE;
    return TRUE;
}
BOOL set_ugly(OC *pcur) {
    g_ionizer_pretty = FALSE;
    return TRUE;
}
BOOL set_pretty(OC *pcur) {
    g_ionizer_pretty = TRUE;
    return TRUE;
}
BOOL set_ascii_only(OC *pcur) {
    g_ionizer_ascii_only = TRUE;
    return TRUE;
}
BOOL set_flush_each(OC *pcur) {
    g_ionizer_flush_each = TRUE;
    return TRUE;
}
BOOL set_include_counts(OC *pcur) {
    g_ionizer_include_counts = TRUE;
    return TRUE;
}
BOOL set_include_type_counts(OC *pcur) {
    g_ionizer_include_type_counts = TRUE;
    return TRUE;
}
BOOL set_output(OC *pc) {
    char * val = opt_get_arg(pc);
    if (val) {
             if (!strcmp(val, "binary"))  return set_write_binary(pc);
        else if (!strcmp(val, "ugly"))    return set_ugly(pc);
        else if (!strcmp(val, "pretty"))  return set_pretty(pc);
        else if (!strcmp(val, "symbols")) return set_write_symtab(pc);
        else if (!strcmp(val, "types"))   return set_include_type_counts(pc);
        else if (!strcmp(val, "counts")) {
            if (!set_write_symtab(pc)) return FALSE;
            return set_include_counts(pc);
        }
        else if (!strcmp(val, "none")) {
            g_ionizer_scan_only = TRUE;
            return TRUE;
        }
        else {
            opt_log_error_line(pc, "valid output values are:");
            opt_log_error_line(pc, "\tbinary  - serialize as binary ion");
            opt_log_error_line(pc, "\tugly    - serialize as ugly (non-pretty) ion text");
            opt_log_error_line(pc, "\tpretty  - serialize as pretty printed ion text (default)");
            opt_log_error_line(pc, "\tnone    - don't output ion, just scan");
            opt_log_error_line(pc, "\ttypes   - output value counts by type");
            opt_log_error_line(pc, "\tsymbols - output symbol table");
            opt_log_error_line(pc, "\tcounts  - output symbol table with occurence counts");
        }
    }
    return FALSE;
}
BOOL set_debug(OC *pcur) {
    g_ionizer_debug  = TRUE;
    g_ionizer_verbose = TRUE;
    return TRUE;
}
BOOL set_trace(OC *pcur) {
    char * val = opt_get_arg(pcur);
    if (val) {
        switch(atoi(val)) {
        case 1: // 1:args
            g_ionizer_dump_args = TRUE;
            break;
        case 2: // parse state
            //_ion_parser_set_debug_flag_states(TRUE);
            //_ion_parser_set_debug_flag_steps(TRUE);
            break;
        case 3: // parse fns
            //_ion_parser_set_debug_flag_actions(TRUE);
            break;
        case 4: // tokens
            //_ion_parser_set_debug_flag_tokens(TRUE);
            break;
		case 5: // function calls
			ion_debug_set_tracing(TRUE);
			break;
        case 6: // timing flag
            g_ion_debug_timer = TRUE;
            break;
        default:
            return FALSE;
        }
    }
    return TRUE;
}
BOOL set_help(OC *pcur) {
    g_ionizer_print_help = TRUE;
    return TRUE;
}


void ionizer_dump_arg_globals(void)
{
    IONIZER_STR_NODE *n;

    fprintf(stderr, "%s: %s\n", "g_ionizer_pretty",       g_ionizer_pretty       ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_ionizer_no_ivm",       g_ionizer_no_ivm       ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_ionizer_write_binary", g_ionizer_write_binary ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_ionizer_write_symtab", g_ionizer_write_symtab ? "true" : "false");

    fprintf(stderr, "%s: %s\n", "g_ionizer_verbose",      g_ionizer_verbose      ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_ionizer_print_help",   g_ionizer_print_help   ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_ionizer_debug",        g_ionizer_debug        ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_ionizer_dump_args",    g_ionizer_dump_args    ? "true" : "false");
  
    fprintf(stderr, "%s: %d\n", "g_ionizer_symtab_version",     g_ionizer_symtab_version);

    fprintf(stderr, "%s: %s\n", "g_ionizer_writer_symtab",      g_ionizer_writer_symtab);
    fprintf(stderr, "%s: %s\n", "g_ionizer_output_symtab_name", g_ionizer_output_symtab_name);

    fprintf(stderr, "g_ionizer_catalogs:\n");
    for (n=g_ionizer_catalogs; n; n = n->next) {
        fprintf(stderr, "\t%s\n", n->str);
    }
    fprintf(stderr, "\n");
}

