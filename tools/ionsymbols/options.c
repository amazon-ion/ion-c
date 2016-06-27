//
//  command line argument option processing routines
//
//  Copyright (c) 2009  - Microquill Inc. - All rights reserved
//  Chris Suver
//

#include "options.h"
#include <stdlib.h>
#include <stdio.h>
#ifdef WIN
#include <io.h>
#endif
#include <malloc.h>
#include <string.h>
#include <ctype.h>


#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

// a global for the list of error messages
// if the user calls us back with extra error information
static OPT_MSG *g_opt_message_head = NULL;
static OPT_MSG *g_opt_message_tail = NULL;
static char    *g_opt_invalid_arg  = NULL;

// internal routines over the argv/argc cursor
struct _opt_cursor {
    int    argc;
    char **argv;
    int    idx;
    int    non_arg_count;
    char **non_argv;
    char  *argp;
    BOOL   is_long;
    OT     arg_type; // type of the arg that was last matched
    char   digits[OPT_MAX_INT_ARG_LEN + 1];
};
void  _opt_option_cursor(OC *cur, int argc, char **argv, char **p_non_args);
char *_opt_next(OC *cur);


// public functions

BOOL opt_process(OPT_DEF *opts, int opt_count, int argc, char **argv) 
{
    return opt_process_return_non_args(opts, opt_count, argc, argv, NULL, NULL); // returns TRUE if ok, FALSE if there's a bad arg
}

BOOL opt_process_return_non_args(OPT_DEF *opts, int opt_count, int argc, char **argv, char **p_non_argv, int *p_non_argc)
{
    BOOL     is_ok = TRUE;
    OPT_DEF *opt;
    char    *arg = NULL;
    int      ii;
    OC       cur, *pcur = &cur;

    if (!opts) return TRUE;

    if ((p_non_argv && !p_non_argc) 
        || 
        (!p_non_argv && p_non_argc)
    ) {
        fprintf(stderr,"ERROR - you either need to ask for non arg, or not, half and half doesn't work.");
        exit(1);
    }

    _opt_option_cursor(pcur,  argc, argv, p_non_argv);
    
    for (arg = _opt_next(pcur); arg; arg = _opt_next(pcur)) {
        is_ok = FALSE;
        for (ii = 0; ii<opt_count; ii++) {
            opt = opts + ii;
            if (pcur->is_long) {
                if (opt->long_name &&!strcmp(opt->long_name, arg)) {
                    pcur->arg_type = opt->type;
                    is_ok = (*opt->fn)(pcur);
                    break;
                }
            }
            else if (opt->short_name == *arg) {
                pcur->arg_type = opt->type;
                is_ok = (*opt->fn)(pcur);
                break;
            }
        }
        if (!is_ok) return FALSE;
        is_ok = TRUE;
    }
    if (!is_ok) {
        fprintf(stderr,"\nunrecognized command arg \"%s\")\n", arg);
    }
    if (g_opt_invalid_arg != NULL) {
        fprintf(stderr,"\nunrecognized command arg \"%s\")\n", g_opt_invalid_arg);
        is_ok = FALSE;
    }
    if (is_ok && p_non_argc) {
        *p_non_argc = pcur->non_arg_count;
    }
    return is_ok;
}


void _opt_option_cursor(OC *cur, int argc, char **argv, char **non_argv)
{
    if (!cur) return;

    cur->argc = argc;
    cur->argv = argv;
    cur->idx  = 1;
    cur->non_arg_count = 0;
    cur->non_argv = non_argv;
    cur->argp = NULL;
    cur->is_long = FALSE;

    return;
}

char *_opt_next(OC *cur)
{
    char *cp;

    if (cur->argp && *cur->argp) {
        return cur->argp++;
    }

    while (cur->idx < cur->argc) {
        cp = cur->argv[cur->idx++];
        switch (*cp) {
        case '-':
            cp++;
            cur->is_long = (*cp == '-');
            if (cur->is_long) {
                cp++;
            }
            cur->argp = cp + 1;
            return cp;
#ifdef DEPRECATED
            // There's a windows (technically a DOS) v Unix (aka Linux)
            // compatibility use case issue here - so I'm just dropping
            // the old style flag marker altogether.
        case '/':
            cp++;
            cur->argp = cp;
            cur->is_long = FALSE;
            return cp;
#endif
        default:
            if (cur->non_argv) {
                cur->non_argv[cur->non_arg_count++] = cp;
            }
            break;
        }
    }
    return NULL;
}

char *opt_get_arg(OC *cur)
{
    char *dst, *src, *ret = NULL;
    OT    type = cur->arg_type;

    src = cur->argp;
    if (!cur->argp || !*cur->argp || cur->is_long) {
        src = cur->argv[cur->idx++];
        if (*src == '-' || *src == '\\') {
            // if it looks like another flag - it's not an arg
            src = NULL;
            cur->idx--;
        }
        cur->argp = NULL; // we've used up this arg in all cases
    }
    if (src) {
        if (type == ot_string) {
            ret = src;
            cur->argp = NULL; // we've used this on up
        }
        else if (type == ot_int) {
            dst = cur->digits;
            while (isdigit(*src)) {
                // hmmm, cheesy, but valid - it's just arg processing after all
                if (dst >= cur->digits + OPT_MAX_INT_ARG_LEN) {
                    break;
                }
                *dst++ = *src++;
            }
            *dst++ = 0;
            ret = cur->digits; // unless we find out otherwise
            if (*src) {
                if (cur->is_long) {
                    ret = NULL; // there were trailing non-digits in the "int"
                    cur->argp = NULL;
                }
                else {
                    cur->argp = src;
                }
            }
            else {
                cur->argp = NULL;
            }
        }
        else {
            opt_log_error_line(cur, "there's invalid type in the option list");
        }
    }
    return ret;
}

void  opt_log_error_line(OC *cur, const char *msg)
{
    OPT_MSG *msg_node = NULL;
    char    *our_copy = NULL;

    if (msg) {
        our_copy = strdup(msg);
        msg_node = (OPT_MSG *)malloc(sizeof(msg_node));
        if (msg_node) {
            msg_node->msg = our_copy;
            if (!g_opt_message_head){
                // if there's not head it's the first node and we'll set it
                g_opt_message_head = msg_node;
            }
            else {
                // if there's list in progress set the current tail's next to the node we're appending
                g_opt_message_tail->next = msg_node;
            }
            // and the new node is always the new tail
            g_opt_message_tail = msg_node;
            msg_node->next = NULL;
        }
    }
    g_opt_invalid_arg = cur->argp;
}

void opt_help(OPT_DEF *opts, int opt_count, char *name, int version, char *title, char *copyright)
{
    int ii, has_short, has_long;
    OPT_DEF *opt;
    OPT_MSG *msg_node;
    char c, s[2], *l;

    fprintf(stderr, "\n");
    if (title) {
        fprintf(stderr, "%s\n", title);
        fprintf(stderr, "%s\n", copyright);
        fprintf(stderr, "Version %d - %s %s\n", version, __DATE__, __TIME__);
    }
    if (opt_count > 0) {
        fprintf(stderr, "usage:\n");
        fprintf(stderr, "\t%s <options>\n", name);
        fprintf(stderr, "\toptions:\n");
        for (ii=0; ii<opt_count; ii++) {
            opt = opts + ii;
            if (opt->hidden) continue;
            c = opt->short_name;
            l = opt->long_name;
            has_short = (c > ' ') ? 1 : 0;
            if (has_short) {
                s[0] = c;
                s[1] = 0;
            }
            has_long  = (l && *l) ? 2 : 0;
            switch(has_short | has_long) {
            case 0: // neither
            default: // huh?
                break;
            case 1: // short only
                fprintf(stderr, "\t-%s - %s\n", s, opt->help);
                break;
            case 2: // long only
                fprintf(stderr, "\t   --%s - %s\n", l, opt->help);
                break;
            case 3: // both
                fprintf(stderr, "\t-%s (--%s) - %s\n", s, l, opt->help);
                break;
            }
        }
    }
    // print any loose messages the user passed us
    if (g_opt_message_head) fprintf(stderr, "\n");
    for (msg_node = g_opt_message_head; msg_node; msg_node = msg_node->next) {
        fprintf(stderr, "%s\n", msg_node->msg);
    }
    return;
}

#ifdef WIN
OPT_MSG *opt_decode_wildcards(OPT_MSG *prev_head, char *name_with_wildcards)
{
    struct _finddata_t fileinfo;
    OPT_MSG *tail, *node;
    intptr_t fh;

    memset(&fileinfo, 0, sizeof(fileinfo));

    if ((fh = _findfirst(name_with_wildcards, &fileinfo)) != -1) {
        for (tail=NULL, node=prev_head; node; tail=node,node=node->next) {} /*just find the tail of the list*/
        do {
            node = (OPT_MSG *)malloc(sizeof(*node));
            if (!node) {
                fprintf(stderr, "ERROR: can't allocate memory for wildcard expansion.\n");
                exit(1);
            }
            node->msg = strdup(fileinfo.name);
            if (!node->msg) {
                fprintf(stderr, "ERROR: can't allocate memory for filename during wildcard expansion.\n");
                exit(1);
            }
            node->next = NULL;
            if (tail) {
                tail->next = node;
            }
            else {
                prev_head = node;
            }
            tail = node;
        } while( _findnext( fh, &fileinfo ) == 0 );
        _findclose( fh );
    }

    return prev_head;
}
#endif
