/*-------------------------------------------------------------------------
 *
 * pg_hap_encoded_attribute.h
 *	  definition of the "Hidden Attribute Partitioning" system catalog
 *	  (pg_hap_encoded_attribute)
 *
 *
 * src/include/catalog/pg_hap_encoded_attribute.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_HAP_ENCODED_ATTRIBUTE
#define PG_HAP_ENCODED_ATTRIBUTE

#include "catalog/genbki.h"
#include "catalog/pg_hap_encoded_attribute_d.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* ----------------
 *		pg_hap_encoded_attribute definition.		cpp turns this into
 *		typedef struct FormData_pg_hap_encoded_attribute
 * ----------------
 */
CATALOG(pg_hap_encoded_attribute,9988,HapEncodedAttributeRelationId)
{
	Oid		haprelid	BKI_LOOKUP(pg_class);
	Oid		hapencodetable;
	int16	hapattrnum;
	int16	hapdescid;
	int32	hapcardinality;
} FormData_pg_hap_encoded_attribute;

/* ----------------
 *		Form_pg_hap_encoded_attribute corresponds to a pointer to a tuple with
 *		the format of pg_hap_encoded_attribute relation.
 * ----------------
 */
typedef FormData_pg_hap_encoded_attribute *Form_pg_hap_encoded_attribute;

DECLARE_UNIQUE_INDEX_PKEY(pg_hap_encoded_attribute_relid_attrnum,9987,HapEncodedAttributeRelidAttrnumIndexId,on pg_hap_encoded_attribute using btree(haprelid oid_ops, hapattrnum int2_ops));

#endif	/* PG_HAP_ENCODED_ATTRIBUTE */
