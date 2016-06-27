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

#include "ionsymbols.h"
#include "options.h"
#ifdef WIN
#include "io.h"
#endif
#include  <assert.h>

//
// local functions that are called to handle the various
// command line arguments as they are encountered
//
BOOL set_update(OC *pcur);
BOOL set_name(OC *pcur);
BOOL set_version(OC *pcur);
BOOL set_catalog_name(OC *pcur);
BOOL set_stats(OC *pcur);
BOOL set_debug(OC *pcur);
BOOL set_timer(OC *pcur);
BOOL set_help(OC *pcur);

OPT_DEF options[] = {
    // type,   short, long_name,     hidden, required,  fn,                 help_message
    { ot_string, 'u', "update",       FALSE, FALSE,     set_update,         "specify an existing symbol table file to Update"},
    { ot_string, 'n', "name",         FALSE, FALSE,     set_name,           "set the output symbol table Name" },
    { ot_int,    'v', "version",      FALSE, FALSE,     set_version,        "set the output symbol table Version" },

    { ot_string, 'c', "catalog",      FALSE, FALSE,     set_catalog_name,   "specify a Catalog file with shared symbol tables"},
    { ot_none,   's', "stats",        FALSE, FALSE,     set_stats,          "include symbols usage Stats in output" },

    { ot_none,   'd', "debug",        TRUE,  FALSE,     set_debug,          "turns on Debug options" },
    { ot_none,   't', "timer",        TRUE,  FALSE,     set_timer,          "turns on Timer" },
    
    { ot_none,   '?', NULL,           TRUE,  FALSE,     set_help,           NULL },
    { ot_none,   'h', "help",         FALSE, FALSE,     set_help,           "prints this Helpful message" }
};
int options_count = sizeof(options) / sizeof(options[0]);

// bonus fns for setting flags for debugging (aka HACK)
extern void _ion_parser_set_debug_flag_states(BOOL is_on);
extern void _ion_parser_set_debug_flag_actions(BOOL is_on);
extern void _ion_parser_set_debug_flag_steps(BOOL is_on);
extern void _ion_parser_set_debug_flag_tokens(BOOL is_on);
extern BOOL g_ion_debug_tracing;

void process_args(int argc, char **argv, char **p_non_argv, int *p_non_argc)
{
    BOOL  is_ok = opt_process_return_non_args(options, options_count, argc, argv, p_non_argv, p_non_argc);
    if (g_print_help || !is_ok) {
        print_help();
    }
    return;
}
void print_help(void) 
{
    opt_help(options
        ,options_count
        , APP_NAME
        , 1
        ,"ionsymbols - Ion symbol table maintanence tool - CSuver"
        ,"Copyright (c) 2009 Amazon.com"
    );
    g_print_help = TRUE;
}

BOOL set_update(OC *pcur) {
    char * val = opt_get_arg(pcur);
    if (!val) return FALSE;
    g_update_symtab = val;
    return TRUE;
}
BOOL set_name(OC *pcur) {
    char * val = opt_get_arg(pcur);
    if (!val) return FALSE;
    g_symtab_name = val;
    return TRUE;
}
BOOL set_version(OC *pcur) {
    char * val = opt_get_arg(pcur);
    if (!val) return FALSE;
    g_symtab_version = atoi(val);
    return TRUE;
}
BOOL set_catalog_name(OC *pcur) {
    STR_NODE *curr, *prev, *cat_node = NULL;
    char     *val = opt_get_arg(pcur);

    if (!val) return FALSE;
    cat_node = (STR_NODE *)malloc(sizeof(*cat_node));
    if (!cat_node) {
        fprintf(stderr, "ERROR - out of memory allocating catalog name linked list nodes!\n");
        exit(1);
    }
    // fill out the node and 
    cat_node->str  = val;
    cat_node->next = NULL;
    for (prev=NULL,curr=g_catalogs; curr; prev=curr, curr=curr->next)
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
        g_catalogs = cat_node;
    }
    return TRUE;
}
BOOL set_stats(OC *pcur) {
    g_include_counts = TRUE;
    return TRUE;
}
BOOL set_debug(OC *pcur) {
    g_debug  = TRUE;
    g_verbose = TRUE;
    g_dump_args = TRUE;
    return TRUE;
}
BOOL set_timer(OC *pcur) {
    g_timer = TRUE;
    return TRUE;
}
BOOL set_help(OC *pcur) {
    g_print_help = TRUE;
    return TRUE;
}

void dump_arg_globals(void)
{
    STR_NODE *n;

    fprintf(stderr, "%s: %s\n", "g_print_help",     g_print_help        ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_dump_args",      g_dump_args         ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_debug",          g_debug             ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_verbose",        g_verbose           ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_timer",          g_timer             ? "true" : "false");
    fprintf(stderr, "%s: %s\n", "g_include_counts", g_include_counts    ? "true" : "false");

    fprintf(stderr, "%s: %s\n", "g_update_symtab",  g_update_symtab);
    fprintf(stderr, "%s: %s\n", "g_symtab_name",    g_symtab_name);

    fprintf(stderr, "%s: %d\n", "g_symtab_version", g_symtab_version);

    fprintf(stderr, "g_catalogs:\n");
    for (n=g_catalogs; n; n = n->next) {
        fprintf(stderr, "\t%s\n", n->str);
    }
    fprintf(stderr, "\n");
}

