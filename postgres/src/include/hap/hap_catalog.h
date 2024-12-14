/*-------------------------------------------------------------------------
 *
 * hap_catalog.h
 *	  POSTGRES hidden attribute partitioning (HAP) catalogs.
 *
 *
 * src/include/hap/hap_catalog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAP_CATALOG_H
#define HAP_CATALOG_H

#include "catalog/pg_hap.h"
#include "catalog/pg_hap_encoded_attribute.h"
#include "catalog/pg_hap_hidden_attribute_desc.h"

/* common */

extern void HapDeleteCatalogTuples(Oid relId);

extern List *HapGetReferencingRelids(Oid confrelid);

extern int HapDeconstructFKeyConst(Oid confrelid, Oid conrelid,
								   int16 *confkeys, int16 *conkeys);


/* pg_hap */

extern void HapInsertRelTuple(Relation rel,
							  int16 hiddenAttrBitsize, int16 hiddenAttrDescCount);

extern void HapDeleteRelTuple(Oid relid);

extern int16 HapRelnameGetHiddenAttrBitsize(const char *relname,
										    Oid relnamespace);

extern int16 HapRelidGetHiddenAttrBitsize(Oid relid);

extern int16 HapRelidGetHiddenAttrBytesize(Oid relid);

extern int16 HapRelnameGetHiddenAttrDescCount(const char *relname,
											  Oid relnamespace);

extern int16 HapRelidGetHiddenAttrDescCount(Oid relid);

extern void HapRelidAddNewHiddenAttrDesc(Oid relid, int16 bitsize);

extern void HapRelidMarkAsEncoded(Oid relid);

extern bool HapRelidIsEncoded(Oid relid);

extern void HapRelidAddHiddenAttrDescBulk(Oid relid,
										  int16 desccount, int16 bitsize);


/* pg_hap_hidden_attribute_desc */

extern int16 HapInsertHiddenAttrDesc(Oid relid, int16 bitsize);

extern void HapDeleteHiddenAttrDesc(Oid relid);

extern void HapInheritHiddenAttrDesc(Oid relid, Oid refrelid);

extern int16 HapGetChildHiddenAttrDescid(Oid relid, Oid confrelid,
										 int16 confdescid);

extern int16 HapGetDescendantHiddenAttrDescid(List *rel_oid_list,
											  int16 ancestor_descid,
											  bool backward);


/* pg_hap_encoded_attribute */

extern void HapInsertEncodedAttribute(Oid relid, Oid transformer,
									  int16 attrnum, int16 descid,
									  int cardinality);

extern void HapDeleteEncodedAttribute(Oid relid);

extern Oid HapGetEncodeTableId(Oid relid, int16 attrnum);

extern int16 HapGetDimensionHiddenAttrDescid(Oid relid, int16 attrnum);

extern int HapGetEncodeTableCardinality(Oid relid, int16 attrnum);

#endif	/* HAP_CATALOG_H */
