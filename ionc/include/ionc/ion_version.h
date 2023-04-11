#ifndef __IONC_VERSION_H__
#define __IONC_VERSION_H__

#include "ion_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Returns a version string containing the major, minor, and patch level in dotted decimal form.
 * The string does not need to be free'd by the caller.
 *
 * Example: "v1.0.0"
 *
 * @return Pointer to version string.
 */
ION_API_EXPORT char const *ion_version(void);

/**
 * Returns a version string containing the major, minor, patch, and revision in a format matching
 * the output from git's describe: vM.m.p-d-gR[-dirty], where M = Major, m = minor, p = Patch,
 * d = commits since tag, R = revision, and -dirty is appended if the repository had been modified
 * at the time of build.
 *
 * This is mostly used for ion-c development and testing.
 */
ION_API_EXPORT char const *ion_version_full(void);

/**
 * Provides ion-c's version information in numeric format by storing the major, minor, and patch level
 * into the locations provided by the caller.
 *
 * @param major A pointer to where to store the major version.
 * @param minor A pointer to where to store the minor version.
 * @param patch A pointer to where to store the patch level.
 * @return IERR_OK if successful.
 */
ION_API_EXPORT iERR ion_version_components(unsigned int *major, unsigned int *minor, unsigned int *patch);

/**
 * Returns a string containing the revision. Currently, the git commit hash.
 *
 * @return A string containing ion-c's revision.
 */
char const *ion_version_revision(void);

#ifdef __cplusplus
}
#endif

#endif /* __IONC_VERSION_H__ */
