#include <stdio.h>

/*
 * Compares two comdb2 semantic versions.
 *
 * On success, stores one of the following values in `cmp_result`:
 * 		-1 if rhs argument is larger
 *		0 if lhs and rhs arguments are equal
 *		1 if lhs argument is larger
 *
 * Returns
 *		0 on success and non-0 on if either version could not be parsed
 */
int compare_semvers(const char * const vers_lhs, const char * const vers_rhs, int * const cmp_result);
