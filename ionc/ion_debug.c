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
// ion library debug facilities
//

#include <ionc/ion_debug.h>

BOOL ion_debug_has_tracing(void)
{
	return g_ion_debug_tracing;
}

void ion_debug_set_tracing(BOOL state)
{
	g_ion_debug_tracing = state;
}
