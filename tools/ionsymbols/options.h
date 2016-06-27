//
//  command line argument option processing routines
//
//  Copyright (c) 2009  - Microquill Inc. - All rights reserved
//  Chris Suver
//

#ifndef OPTION_H_INCLUDED
#define OPTION_H_INCLUDED

#define OPT_MAX_INT_ARG_LEN 30

#ifndef BOOL
#define BOOL int
#endif

#ifndef GLOBAL
#define GLOBAL extern
#define OPT_INITTO(x)
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef enum _opt_type {
    ot_none,
    ot_int,
    ot_string,
    ot_non_arg
} OT;

typedef struct _opt_cursor OC;
typedef BOOL (*OPT_ARG_FN)(OC *pcur);

typedef struct _opt_def {
    OT           type;
    char         short_name;
    char        *long_name;
    BOOL         hidden;
    BOOL         required;
    OPT_ARG_FN   fn;
    char        *help;
} OPT_DEF;

typedef struct _opt_msg OPT_MSG;
struct _opt_msg {
    char    *msg;
    OPT_MSG *next;
};

BOOL  opt_process(OPT_DEF *opts, int opt_count, int argc, char **argv); // returns TRUE if ok, FALSE if there's a bad arg
BOOL  opt_process_return_non_args(OPT_DEF *opts, int opt_count, int argc, char **argv, char **p_non_argv, int *p_non_argc); // returns TRUE if ok, FALSE if there's a bad arg
void  opt_log_error_line(OC *cur, const char *msg); // sets options as in error and appends this string to the error message list for help (if the user calls help)
void  opt_help   (OPT_DEF *opts, int opt_count, char *name, int version, char *title, char *copyright);
char *opt_get_arg(OC *cur);
#ifdef WIN
OPT_MSG *opt_decode_wildcards(OPT_MSG *prev_head, char *name_with_wildcards);
#endif

#ifdef __cplusplus
}
#endif

#endif

