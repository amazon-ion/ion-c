#include <ion_debug.h>

static int ASSERT_BUFFER_SIZE = 1024;

#define ASSERT_ERROR(message, file, line) fprintf(stderr, "%s (%s:%d)", (message), ion_helper_short_filename((file)), (line))

#define ASSERT_EQUALS_INT(expected, actual, message) IONCHECK(_assert_equals_int((expected), (actual), (message), __FILE__, __LINE__))

static inline iERR _assert_equals_int(int expected, int actual, char* message, char* file, int line) {
    if (expected == actual) {
        return IERR_OK;
    } else {
        char* buffer = calloc(ASSERT_BUFFER_SIZE, sizeof(char));
        if (message != NULL) {
            snprintf(buffer, ASSERT_BUFFER_SIZE, "%s: Expected %i but was %i", message, expected, actual);
        } else {
            snprintf(buffer, ASSERT_BUFFER_SIZE, "Expected %i but was %i", message, expected, actual);
        }
        ASSERT_ERROR(buffer, file, line);
        return IERR_INVALID_ARG;
    }
}
