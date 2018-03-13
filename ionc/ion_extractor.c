/*
 * Copyright 2012-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#include <inc/ion_extractor.h>
#include "ion_extractor_impl.h"

#if ION_EXTRACTOR_MAX_NUM_PATHS > ION_EXTRACTOR_MAX_NUM_PATHS_THRESHOLD
    #define ION_EXTRACTOR_ACTIVATE_ALL_PATHS(map) memset(map, 0xFF, ION_EXTRACTOR_PATH_BITMAP_BYTE_SIZE)
    #define ION_EXTRACTOR_DEACTIVATE_ALL_PATHS(map) memset(map, 0, ION_EXTRACTOR_PATH_BITMAP_BYTE_SIZE)
    #define ION_EXTRACTOR_INDEX_PATH(map, path_index) (map)[(path_index) / ION_EXTRACTOR_MAX_NUM_PATHS_THRESHOLD]
    static ION_EXTRACTOR_ACTIVE_PATH_MAP _ION_EXTRACTOR_PATH_MAP_ZERO;
    #define ION_EXTRACTOR_ANY_PATHS_ACTIVE(map) memcmp(map, _ION_EXTRACTOR_PATH_MAP_ZERO, ION_EXTRACTOR_PATH_BITMAP_BYTE_SIZE)
    #define ION_EXTRACTOR_PATH_MAP_ZERO _ION_EXTRACTOR_PATH_MAP_ZERO
    #define ION_EXTRACTOR_PATH_BIT_SHIFT(path_index) (1L << ((path_index) % ION_EXTRACTOR_MAX_NUM_PATHS_THRESHOLD))
#else
    #define ION_EXTRACTOR_ACTIVATE_ALL_PATHS(map) map = (ION_EXTRACTOR_ACTIVE_PATH_MAP)0xFFFFFFFFFFFFFFFF
    #define ION_EXTRACTOR_DEACTIVATE_ALL_PATHS(map) map = (ION_EXTRACTOR_ACTIVE_PATH_MAP)0
    #define ION_EXTRACTOR_INDEX_PATH(map, path_index) map
    #define ION_EXTRACTOR_ANY_PATHS_ACTIVE(map) map != 0
    #define ION_EXTRACTOR_PATH_MAP_ZERO 0
    #define ION_EXTRACTOR_PATH_BIT_SHIFT(path_index) (1L << (path_index))
#endif

#define ION_EXTRACTOR_ACTIVATE_PATH(map, path_index) ION_EXTRACTOR_INDEX_PATH(map, path_index) |= ION_EXTRACTOR_PATH_BIT_SHIFT(path_index)
#define ION_EXTRACTOR_DEACTIVATE_PATH(map, path_index) ION_EXTRACTOR_INDEX_PATH(map, path_index) &= ~ION_EXTRACTOR_PATH_BIT_SHIFT(path_index)
#define ION_EXTRACTOR_IS_PATH_ACTIVE(map, path_index) ION_EXTRACTOR_INDEX_PATH(map, path_index) & ION_EXTRACTOR_PATH_BIT_SHIFT(path_index)

#define ION_EXTRACTOR_GET_COMPONENT(extractor, path_depth, path_index) \
    &(extractor)->_path_components[(path_depth) * (extractor)->_options.max_num_paths + (path_index)]

#define ION_EXTRACTOR_FIELD_ANNOTATION "$ion_extractor_field"
#define ION_EXTRACTOR_WILDCARD "*"

iERR ion_extractor_open(hEXTRACTOR *extractor, ION_EXTRACTOR_OPTIONS *options) {
    iENTER;
    ION_EXTRACTOR *pextractor = NULL;
    SIZE len;
    ASSERT(extractor);

    if (options) {
        if (options->max_num_paths > ION_EXTRACTOR_MAX_NUM_PATHS || options->max_num_paths < 1) {
            FAILWITHMSG(IERR_INVALID_ARG, "Extractor's max_num_paths must be in [1, ION_EXTRACTOR_MAX_NUM_PATHS].");
        }
        if (options->max_path_length > ION_EXTRACTOR_MAX_PATH_LENGTH || options->max_path_length < 1) {
            FAILWITHMSG(IERR_INVALID_ARG, "Extractor's max_path_length must be in [1, ION_EXTRACTOR_MAX_PATH_LENGTH].");
        }
    }

    len = sizeof(ION_EXTRACTOR);
    pextractor = (ION_EXTRACTOR *)ion_alloc_owner(len);
    *extractor = pextractor;
    if (!pextractor) {
        FAILWITH(IERR_NO_MEMORY);
    }
    memset(pextractor, 0, len);
    pextractor->_options.max_num_paths = (options) ? options->max_num_paths : (ION_EXTRACTOR_SIZE)ION_EXTRACTOR_MAX_NUM_PATHS;
    pextractor->_options.max_path_length = (options) ? options->max_path_length : (ION_EXTRACTOR_SIZE)ION_EXTRACTOR_MAX_PATH_LENGTH;
    pextractor->_options.match_relative_paths = (options) ? options->match_relative_paths : false;
    pextractor->_options.match_case_insensitive = (options) ? options->match_case_insensitive : false;

    iRETURN;
}

iERR ion_extractor_close(ION_EXTRACTOR *extractor) {
    iENTER;
    ASSERT(extractor);
    // Frees associated resources (path descriptors, copied field strings), then frees the extractor
    // itself.
    ion_free_owner(extractor);
    iRETURN;
}

iERR ion_extractor_path_create(ION_EXTRACTOR *extractor, ION_EXTRACTOR_SIZE path_length, ION_EXTRACTOR_CALLBACK callback,
                               void *user_context, ION_EXTRACTOR_PATH_DESCRIPTOR **p_path) {
    iENTER;
    ION_EXTRACTOR_MATCHER *matcher;
    ION_EXTRACTOR_PATH_DESCRIPTOR *path;

    ASSERT(extractor);
    ASSERT(callback);
    ASSERT(p_path);

    if (extractor->_matchers_length >= extractor->_options.max_num_paths) {
        FAILWITHMSG(IERR_INVALID_STATE, "Too many registered paths.");
    }
    if (path_length > extractor->_options.max_path_length || path_length < 0) {
        FAILWITHMSG(IERR_INVALID_ARG, "Illegal number of path components.");
    }
    // This will be freed by ion_free_owner during ion_extractor_close.
    path = ion_alloc_with_owner(extractor, sizeof(ION_EXTRACTOR_PATH_DESCRIPTOR));
    if (!path) {
        FAILWITH(IERR_NO_MEMORY);
    }
    path->_path_length = path_length;
    path->_path_id = extractor->_matchers_length++;
    if (path_length > 0) {
        ION_EXTRACTOR_ACTIVATE_PATH(extractor->_path_in_progress, path->_path_id);
    }
    else {
        ION_EXTRACTOR_ACTIVATE_PATH(extractor->_depth_zero_active_paths, path->_path_id);
    }
    path->_extractor = extractor;
    path->_current_length = 0;
    matcher = &extractor->_matchers[path->_path_id];
    matcher->_callback = callback;
    matcher->_user_context = user_context;
    matcher->_path = path;
    *p_path = path;

    iRETURN;
}

iERR _ion_extractor_path_append_helper(ION_EXTRACTOR_PATH_DESCRIPTOR *path, ION_EXTRACTOR_PATH_COMPONENT **p_component) {
    iENTER;
    ION_EXTRACTOR_PATH_COMPONENT *component;
    ION_EXTRACTOR *extractor;

    if (!path) {
        FAILWITHMSG(IERR_INVALID_ARG, "Path must be non-null.");
    }

    extractor = path->_extractor;

    ASSERT(p_component);
    ASSERT(extractor);

    if (!extractor->_path_in_progress || extractor->_matchers_length <= 0) {
        FAILWITHMSG(IERR_INVALID_STATE, "No path is in progress.");
    }

    if (path->_current_length >= extractor->_options.max_path_length || path->_current_length >= path->_path_length) {
        FAILWITHMSG(IERR_INVALID_STATE, "Path is too long.");
    }

    component = ION_EXTRACTOR_GET_COMPONENT(extractor, path->_current_length, path->_path_id);
    component->_is_terminal = (++path->_current_length == path->_path_length);
    if (component->_is_terminal) {
        ION_EXTRACTOR_DEACTIVATE_PATH(extractor->_path_in_progress, path->_path_id);
    }
    *p_component = component;
    iRETURN;
}

iERR ion_extractor_path_append_field(ION_EXTRACTOR_PATH_DESCRIPTOR *path, ION_STRING *value) {
    iENTER;
    ION_EXTRACTOR_PATH_COMPONENT *component;
    if (!value) {
        FAILWITHMSG(IERR_INVALID_ARG, "Field string must not be null.");
    }
    IONCHECK(_ion_extractor_path_append_helper(path, &component));
    // Note: this is an allocation, with extractor as the memory owner. This allocated memory is freed by
    // `ion_free_owner` during `ion_extractor_close`. Each of these occupies space in a contiguous block assigned
    // to the extractor.
    IONCHECK(ion_string_copy_to_owner(path->_extractor, &component->_value.text, value));
    component->_type = FIELD;
    iRETURN;
}

iERR ion_extractor_path_append_ordinal(ION_EXTRACTOR_PATH_DESCRIPTOR *path, POSITION value) {
    iENTER;
    ION_EXTRACTOR_PATH_COMPONENT *component;
    if (value < 0) {
        FAILWITHMSG(IERR_INVALID_ARG, "Ordinal cannot be negative.");
    }
    IONCHECK(_ion_extractor_path_append_helper(path, &component));
    component->_value.ordinal = value;
    component->_type = ORDINAL;
    iRETURN;
}

iERR ion_extractor_path_append_wildcard(ION_EXTRACTOR_PATH_DESCRIPTOR *path) {
    iENTER;
    ION_EXTRACTOR_PATH_COMPONENT *component;
    IONCHECK(_ion_extractor_path_append_helper(path, &component));
    component->_type = WILDCARD;
    iRETURN;
}

iERR ion_extractor_path_create_from_ion(ION_EXTRACTOR *extractor, ION_EXTRACTOR_CALLBACK callback,
                                        void *user_context, BYTE *ion_data, SIZE ion_data_length,
                                        ION_EXTRACTOR_PATH_DESCRIPTOR **p_path) {
    iENTER;
    ION_READER *reader = NULL;
    ION_READER_OPTIONS options;
    ION_TYPE type;
    ION_STRING text, field_annotation, wildcard;
    BOOL has_annotations;
    ION_EXTRACTOR_PATH_COMPONENT components[ION_EXTRACTOR_MAX_PATH_LENGTH], *component;
    ION_EXTRACTOR_SIZE path_length = 0, i;
    ION_EXTRACTOR_PATH_DESCRIPTOR *path;

    ASSERT(extractor);
    ASSERT(callback);
    ASSERT(ion_data_length > 0);
    ASSERT(p_path);

    field_annotation.value = (BYTE *)ION_EXTRACTOR_FIELD_ANNOTATION;
    field_annotation.length = (int32_t)strlen(ION_EXTRACTOR_FIELD_ANNOTATION);
    wildcard.value = (BYTE *)ION_EXTRACTOR_WILDCARD;
    wildcard.length = 1;

    memset(&options, 0, sizeof(ION_READER_OPTIONS));
    options.max_container_depth = (extractor->_options.max_path_length < MIN_WRITER_STACK_DEPTH)
                                  ? MIN_WRITER_STACK_DEPTH : extractor->_options.max_path_length;
    IONCHECK(ion_reader_open_buffer(&reader, ion_data, ion_data_length, &options));
    IONCHECK(ion_reader_next(reader, &type));
    if (type != tid_SEXP && type != tid_LIST) {
        FAILWITHMSG(IERR_INVALID_ARG, "Improper path format.");
    }
    IONCHECK(ion_reader_step_in(reader));
    for (;;) {
        component = &components[path_length];
        IONCHECK(ion_reader_next(reader, &type));
        if (type == tid_EOF) {
            break;
        }
        path_length++;
        switch(ION_TYPE_INT(type)) {
            case tid_INT_INT:
                component->_type = ORDINAL;
                IONCHECK(ion_reader_read_int64(reader, &component->_value.ordinal));
                break;
            case tid_SYMBOL_INT:
            case tid_STRING_INT:
                IONCHECK(ion_reader_read_string(reader, &text));
                IONCHECK(ion_string_copy_to_owner(extractor, &component->_value.text, &text));
                component->_type = FIELD;
                if (ION_STRING_EQUALS(&wildcard, &text)) {
                    IONCHECK(ion_reader_has_any_annotations(reader, &has_annotations));
                    if (has_annotations) {
                        IONCHECK(ion_reader_get_an_annotation(reader, 0, &text));
                        if (ION_STRING_EQUALS(&field_annotation, &text)) {
                            break;
                        }
                    }
                    component->_type = WILDCARD;
                }
                break;
            default:
                FAILWITHMSG(IERR_INVALID_ARG, "Improper path format.");
        }
    }
    IONCHECK(ion_reader_step_out(reader));
    IONCHECK(ion_reader_next(reader, &type));
    if (type != tid_EOF) {
        FAILWITHMSG(IERR_INVALID_ARG, "Improper path format: more than one top-level value.");
    }
    IONCHECK(ion_extractor_path_create(extractor, path_length, callback, user_context, &path));
    for (i = 0; i < path_length; i++) {
        component = &components[i];
        switch (component->_type) {
            case ORDINAL:
                IONCHECK(ion_extractor_path_append_ordinal(path, component->_value.ordinal));
                break;
            case FIELD:
                IONCHECK(ion_extractor_path_append_field(path, &component->_value.text));
                break;
            case WILDCARD:
                IONCHECK(ion_extractor_path_append_wildcard(path));
                break;
            default:
                FAILWITH(IERR_INVALID_STATE);
        }
    }

    *p_path = path;
    iRETURN;
}

bool _ion_extractor_string_equals_nocase(ION_STRING *lhs, ION_STRING *rhs) {
    if (lhs == rhs) {
        return true;
    }
    if (lhs->length != rhs->length) {
        return false;
    }
    for (size_t i = 0; i < lhs->length; i++) {
        if (tolower((char)lhs->value[i]) != tolower((char)rhs->value[i])) {
            return false;
        }
    }
    return true;
}

iERR _ion_extractor_evaluate_field_predicate(ION_READER *reader, ION_EXTRACTOR_PATH_COMPONENT *path_component,
                                             bool is_case_insensitive, bool *matches) {
    iENTER;
    ION_STRING field_name;

    ASSERT(path_component->_type == FIELD);

    IONCHECK(ion_reader_get_field_name(reader, &field_name));
    if (is_case_insensitive) {
        *matches = _ion_extractor_string_equals_nocase(&field_name, &path_component->_value.text);
    }
    else {
        *matches = ION_STRING_EQUALS(&field_name, &path_component->_value.text);
    }
    iRETURN;
}

iERR _ion_extractor_evaluate_predicate(ION_READER *reader, ION_EXTRACTOR_PATH_COMPONENT *path_component,
                                       POSITION ordinal, bool is_case_insensitive, bool *matches) {
    iENTER;

    ASSERT(reader);
    ASSERT(path_component);
    ASSERT(matches);

    *matches = false;

    switch (path_component->_type) {
        case FIELD:
            IONCHECK(_ion_extractor_evaluate_field_predicate(reader, path_component, is_case_insensitive, matches));
            break;
        case ORDINAL:
            *matches = ordinal == path_component->_value.ordinal;
            break;
        case WILDCARD:
            // TODO different types of wildcards?
            *matches = true;
            break;
        default:
            FAILWITH(IERR_INVALID_STATE);
    }
    iRETURN;
}

iERR _ion_extractor_dispatch_match(ION_EXTRACTOR *extractor, ION_READER *reader, ION_EXTRACTOR_SIZE matcher_index,
                                   ION_EXTRACTOR_CONTROL *control) {
    iENTER;
    ION_EXTRACTOR_MATCHER *matcher;
    SIZE old_depth, new_depth;

    matcher = &extractor->_matchers[matcher_index];
    IONCHECK(ion_reader_get_depth(reader, &old_depth));
    IONCHECK(matcher->_callback(reader, matcher->_path, matcher->_user_context, control));
    IONCHECK(ion_reader_get_depth(reader, &new_depth));
    if (old_depth != new_depth) {
        FAILWITHMSG(IERR_INVALID_STATE, "Reader must be positioned at same depth after callback returns.");
    }

    iRETURN;
}

iERR _ion_extractor_evaluate_predicates(ION_EXTRACTOR *extractor, ION_READER *reader, SIZE depth, POSITION ordinal,
                                        ION_EXTRACTOR_CONTROL *control,
                                        ION_EXTRACTOR_ACTIVE_PATH_MAP previous_depth_actives,
                                        ION_EXTRACTOR_ACTIVE_PATH_MAP *current_depth_actives) {
    iENTER;
    ION_EXTRACTOR_SIZE i;
    bool matches;
    ION_EXTRACTOR_PATH_COMPONENT *path_component = NULL;
    ASSERT(current_depth_actives);
    ASSERT(depth >= 0);
    // This depth should not have been stepped into if nothing matched at the previous depth.
    ASSERT(depth > 0 ? previous_depth_actives != 0 : TRUE);
    // NOTE: The following is not a user error because reaching this point requires an active path at this depth and
    // depths above the max path length are rejected at construction.
    ASSERT(depth <= extractor->_options.max_path_length);

    *control = ion_extractor_control_next();
    for (i = 0; i < extractor->_matchers_length; i++) {
        if (ION_EXTRACTOR_IS_PATH_ACTIVE(previous_depth_actives, i)) {
            if (depth == 0) {
                matches = true;
            }
            else {
                path_component = ION_EXTRACTOR_GET_COMPONENT(extractor, depth - 1, i);
                ASSERT(path_component);
                IONCHECK(_ion_extractor_evaluate_predicate(reader, path_component, ordinal,
                                                           extractor->_options.match_case_insensitive, &matches));
            }
            if (matches) {
                if (!path_component || path_component->_is_terminal) {
                    // Matches at depth == 0 require a length-zero path. This is treated as a special case to keep the
                    // extractor's path components array as dense as possible -- length zero paths are not stored, and
                    // are instead treated as NULL here.
                    ASSERT((!path_component) ? depth == 0 : TRUE);
                    IONCHECK(_ion_extractor_dispatch_match(extractor, reader, i, control));
                    if (*control) {
                        if (*control > depth) {
                            FAILWITHMSG(IERR_INVALID_STATE, "Received a control instruction to step out past current depth.")
                        }
                        SUCCEED();
                    }
                }
                ION_EXTRACTOR_ACTIVATE_PATH(*current_depth_actives, i);
            }
        }
    }

    iRETURN;
}

iERR _ion_extractor_match_helper(hEXTRACTOR extractor, ION_READER *reader, SIZE depth,
                                 ION_EXTRACTOR_ACTIVE_PATH_MAP previous_depth_actives,
                                 ION_EXTRACTOR_CONTROL *control) {
    iENTER;
    ION_TYPE t;
    POSITION ordinal = 0;
    ION_EXTRACTOR_ACTIVE_PATH_MAP current_depth_actives;

    for (;;) {
        IONCHECK(ion_reader_next(reader, &t));
        if (t == tid_EOF) {
            break;
        }
        // Each value at depth N can match any active partial path from depth N - 1.
        ION_EXTRACTOR_DEACTIVATE_ALL_PATHS(current_depth_actives);
        if (depth == 0) {
            // Everything matches at depth 0.
            ION_EXTRACTOR_ACTIVATE_ALL_PATHS(current_depth_actives);
        }
        IONCHECK(_ion_extractor_evaluate_predicates(extractor, reader, depth, ordinal, control,
                                                    previous_depth_actives, &current_depth_actives));
        if (*control) {
            *control -= 1;
            SUCCEED();
        }
        ordinal++;
        switch(ION_TYPE_INT(t)) {
            case tid_NULL_INT:
            case tid_BOOL_INT:
            case tid_INT_INT:
            case tid_FLOAT_INT:
            case tid_DECIMAL_INT:
            case tid_TIMESTAMP_INT:
            case tid_SYMBOL_INT:
            case tid_STRING_INT:
            case tid_CLOB_INT:
            case tid_BLOB_INT:
                continue;
            case tid_LIST_INT:
            case tid_SEXP_INT:
            case tid_STRUCT_INT:
                if (ION_EXTRACTOR_ANY_PATHS_ACTIVE(current_depth_actives)) {
                    IONCHECK(ion_reader_step_in(reader));
                    IONCHECK(_ion_extractor_match_helper(extractor, reader, depth + 1, current_depth_actives, control));
                    IONCHECK(ion_reader_step_out(reader));
                    if (*control) {
                        *control -= 1;
                        SUCCEED();
                    }
                }
                continue;
            default:
                FAILWITH(IERR_INVALID_STATE);
        }
    }
    iRETURN;
}


iERR ion_extractor_match(ION_EXTRACTOR *extractor, ION_READER *reader) {
    iENTER;
    SIZE depth;
    ION_EXTRACTOR_CONTROL control = ion_extractor_control_next();

    ASSERT(extractor);
    ASSERT(reader);

    if (ION_EXTRACTOR_ANY_PATHS_ACTIVE(extractor->_path_in_progress)) {
        FAILWITHMSG(IERR_INVALID_STATE, "Cannot start matching with a path in progress.");
    }

    IONCHECK(ion_reader_get_depth(reader, &depth));
    if (!extractor->_options.match_relative_paths && depth != 0) {
        FAILWITHMSG(IERR_INVALID_STATE, "Reader must be at depth 0 to start matching.");
    }
    if (extractor->_matchers_length) {
        IONCHECK(_ion_extractor_match_helper(extractor, reader, 0, extractor->_depth_zero_active_paths, &control));
    }
    iRETURN;
}
