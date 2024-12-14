/*-------------------------------------------------------------------------
 *
 * hap_am.h
 *	  POSTGRES hidden attribute partitioning (HAP) access method definitions.
 *
 *
 * src/include/hap/hap_am.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAP_AM_H
#define HAP_AM_H

/* Oid of HAP handler that is registered in pg_proc.dat */
#define HAP_TABLE_ACCESS_METHOD_HANDLER_OID	(6)

extern bool HapCheckAmUsingName(const char *accessMethodName);

extern bool HapCheckAmUsingRelId(Oid relId);

#endif	/* HAP_AM_H */
