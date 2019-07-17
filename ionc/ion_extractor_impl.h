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

#ifndef ION_EXTRACTOR_IMPL_H
#define ION_EXTRACTOR_IMPL_H

#include "ion_internal.h"
#include <ionc/ion_extractor.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef ION_EXTRACTOR_MAX_NUM_PATHS_THRESHOLD
    #undef ION_EXTRACTOR_MAX_NUM_PATHS_THRESHOLD
#endif

/**
 * Threshold over which more than one unit is required to compose the registered paths bitmap. Registered paths are
 * assigned a bit index in a bitmap.
 * NOTE: this is a constant that may not be redefined by the user.
 */
#define ION_EXTRACTOR_MAX_NUM_PATHS_THRESHOLD 64

/**
 * A bit map representing matching paths at a particular path depth. If the bit at index N is set, it means the path
 * with ID = N is active. If zero, there are no paths active at this depth, and the extractor is free to skip and step
 * out.
 */
#if ION_EXTRACTOR_MAX_NUM_PATHS > ION_EXTRACTOR_MAX_NUM_PATHS_THRESHOLD
    #define ION_EXTRACTOR_PATH_BITMAP_SIZE \
        ((ION_EXTRACTOR_MAX_NUM_PATHS / ION_EXTRACTOR_MAX_NUM_PATHS_THRESHOLD) \
            + (ION_EXTRACTOR_MAX_NUM_PATHS % ION_EXTRACTOR_MAX_NUM_PATHS_THRESHOLD != 0 ? 1 : 0))
    #define ION_EXTRACTOR_PATH_BITMAP_BYTE_SIZE (ION_EXTRACTOR_PATH_BITMAP_SIZE * sizeof(uint_fast64_t))
    typedef uint_fast64_t ION_EXTRACTOR_ACTIVE_PATH_MAP[ION_EXTRACTOR_PATH_BITMAP_SIZE];
#else
    typedef uint_fast64_t ION_EXTRACTOR_ACTIVE_PATH_MAP;
#endif

/**
 * A descriptor for a path for the extractor to match.
 */
struct _ion_extractor_path_descriptor {
    /**
     * A unique identifier for this path.
     */
    ION_EXTRACTOR_SIZE _path_id;

    /**
     * The number of components in the path.
     */
    ION_EXTRACTOR_SIZE _path_length;

    /**
     * The current length of the path. In order for the path to be valid, this must be equivalent to `_path_length` when
     * the user is finished building the path.
     */
    ION_EXTRACTOR_SIZE _current_length;

    /**
     * The extractor to which this path is registered.
     */
    ION_EXTRACTOR *_extractor;

};

/**
 * A path component type.
 */
typedef enum _ion_extractor_path_component_type {
    /**
     * This component provides text which must exactly match a field name within the struct at this component's
     * depth in the path.
     */
    FIELD       = 0,

    /**
     * This component provides an ordinal, which must exactly match an index in the collection at this component's
     * depth in the path.
     */
    ORDINAL     = 1,

    /**
     * This component provides text which matches any value at this component's depth in the path.
     */
    WILDCARD    = 2,

} ION_EXTRACTOR_PATH_COMPONENT_TYPE;

/**
 * A path component, which can represent a particular field, ordinal, or wildcard.
 */
typedef struct _ion_extractor_path_component {
    /**
     * `false` if there are more components in the path; `true` if this is the last component in the path.
     * If this component is terminal and it matches the current value, the matcher's callback should be
     * invoked. If it is not terminal, but it matches the current element, then the matcher should remain active
     * at this depth. If this component doesn't match the current element, it should be marked inactive.
     *
     * NOTE: it is possible to calculate whether a component is terminal (`true` if the component's depth is equal to
     * the matcher's path's length), but storing it may be cheaper, as calculating it would require accessing the
     * matcher's path's length in a disparate memory location each time a component is accessed.
     */
    bool _is_terminal;

    /**
     * The type of the component: FIELD, ORDINAL, or WILDCARD.
     */
    ION_EXTRACTOR_PATH_COMPONENT_TYPE _type;

    /**
     * The value of the component. If the component's type is FIELD or WILDCARD, `text` must be valid.
     * If the component's type is ORDINAL, `ordinal` must be valid.
     */
    union {
        ION_STRING  text;
        POSITION    ordinal;
    } _value;

} ION_EXTRACTOR_PATH_COMPONENT;

/**
 * Stores the data needed to convey a match to the user. One ION_EXTRACTOR_MATCHER is created per path.
 *
 * NOTE: the user retains memory ownership of `_callback` and `_user_context`.
 */
typedef struct _ion_extractor_matcher {
    /**
     * The path to match.
     */
    ION_EXTRACTOR_PATH_DESCRIPTOR *_path;

    /**
     * The callback to invoke when the path matches.
     */
    ION_EXTRACTOR_CALLBACK _callback;

    /**
     * The opaque user context to provide to the callback upon match.
     */
    void *_user_context;

} ION_EXTRACTOR_MATCHER;

struct _ion_extractor {

    /**
     * The configuration options.
     */
    ION_EXTRACTOR_OPTIONS _options;

    /**
     * Nonzero if the user has started, but not finished, a path. When nonzero, the user cannot start matching.
     * When bit i is set, the path with _path_id=i is in progress, meaning that its actual length does not match its
     * declared length.
     */
    ION_EXTRACTOR_ACTIVE_PATH_MAP _path_in_progress;

    /**
     * When bit i is set, the path with _path_id=i has zero length, meaning that it matches every value that is
     * considered depth zero by the extractor. If `_options.match_relative_paths=false` this must be absolute depth
     * zero; otherwise, this is the depth at which the reader is positioned at the start of matching.
     */
    ION_EXTRACTOR_ACTIVE_PATH_MAP _depth_zero_active_paths;

    /**
     * Path components from all registered paths organized by depth. Components at depth 1 begin at index 0, components
     * at depth 2 begin at index ION_EXTRACTOR_MAX_NUM_PATHS, and so on. There is a maximum of
     * ION_EXTRACTOR_MAX_PATH_LENGTH depths. This organization mimics access order: when determining matches
     * at depth N, all partial paths that matched at depth N - 1 must have their components at depth N accessed and
     * tested.
     */
    ION_EXTRACTOR_PATH_COMPONENT _path_components[ION_EXTRACTOR_MAX_PATH_LENGTH * ION_EXTRACTOR_MAX_NUM_PATHS];

    /**
     * The number of valid elements in `_matchers`.
     */
    ION_EXTRACTOR_SIZE _matchers_length;

    /**
     * A matcher for a particular path, indexed by path ID.
     */
    ION_EXTRACTOR_MATCHER _matchers[ION_EXTRACTOR_MAX_NUM_PATHS];

};

#ifdef __cplusplus
}
#endif

#endif //ION_EXTRACTOR_IMPL_H
