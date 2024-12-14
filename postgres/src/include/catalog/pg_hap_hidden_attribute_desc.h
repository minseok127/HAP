/*-------------------------------------------------------------------------
 *
 * pg_hap_hidden_attribute_desc.h
 *	  definition of the "Hidden Attribute Descriptor" system catalog
 *	  (pg_hap_hidden_attribute_desc)
 *
 *
 * src/include/catalog/pg_hap_hidden_attribute_desc.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_HAP_HIDDEN_ATTRIBUTE_DESC_H
#define PG_HAP_HIDDEN_ATTRIBUTE_DESC_H

#include "catalog/genbki.h"
#include "catalog/pg_hap_hidden_attribute_desc_d.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* ----------------
 *		pg_hap_hidden_attribute_desc definition.	cpp turns this into
 *		typedef struct FormData_pg_hap_hidden_attribute_desc
 * ----------------
 */
CATALOG(pg_hap_hidden_attribute_desc,9991,HapHiddenAttributeDescRelationId)
{
	Oid			haprelid		BKI_LOOKUP(pg_class);
	Oid			hapconfrelid	BKI_LOOKUP(pg_class);
	int16		hapstartbit;
	int16		hapbitsize;
	int16		hapdescid;
	int16		hapconfdescid;
	int16		happartkeyidx;
} FormData_pg_hap_hidden_attribute_desc;

/* ----------------
 *		Form_pg_hap_hidden_attribute_desc corresponds to a pointer to a tuple with
 *		the format of pg_hap_hidden_attribute_desc relation.
 * ----------------
 */
typedef FormData_pg_hap_hidden_attribute_desc *Form_pg_hap_hidden_attribute_desc;

DECLARE_UNIQUE_INDEX_PKEY(pg_hap_hidden_attribute_desc_relid_descid,9989,HapHiddenAttributeDescRelidDescidIndexId,on pg_hap_hidden_attribute_desc using btree(haprelid oid_ops, hapdescid int2_ops));

DECLARE_UNIQUE_INDEX(pg_hap_hidden_attribute_desc_relid_confrelid_confdescid,9990,HapHiddenAttributeDescRelidConfrelidConfdescidIndexId,on pg_hap_hidden_attribute_desc using btree(haprelid oid_ops, hapconfrelid oid_ops, hapconfdescid int2_ops));

#endif	/* PG_HAP_HIDDEN_ATTRIBUTE_DESC_H */
