/*-------------------------------------------------------------------------
 *
 * pg_hap.h
 *	  definition of the "Hidden Attribute Partitioning" system catalog (pg_hap)
 *
 *
 * src/include/catalog/pg_hap.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_HAP_H
#define PG_HAP_H

#include "catalog/genbki.h"
#include "catalog/pg_hap_d.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* ----------------
 *		pg_hap definition.	cpp turns this into
 *		typedef struct FormData_pg_hap
 * ----------------
 */
CATALOG(pg_hap,9999,HapRelationId)
{
	Oid			haprelid		BKI_LOOKUP(pg_class);
	Oid			happartmapid	BKI_DEFAULT(0);
	Oid			haprelnamespace;
	int16		hapbitsize;
	int16		hapdesccount;
	bool		hapencoded		BKI_DEFAULT(f);
	NameData	haprelname;
} FormData_pg_hap;

/* ----------------
 *		Form_pg_hap corresponds to a pointer to a tuple with
 *		the format of pg_hap relation.
 * ----------------
 */
typedef FormData_pg_hap *Form_pg_hap;

DECLARE_UNIQUE_INDEX_PKEY(pg_hap_haprelid_index,9998,HapRelIdIndexId,on pg_hap using btree(haprelid oid_ops));

DECLARE_UNIQUE_INDEX(pg_hap_haprelname_nsp_index,9997,HapNameNspIndexId,on pg_hap using btree(haprelname name_ops, haprelnamespace oid_ops));

#endif	/* PG_HAP_H */
