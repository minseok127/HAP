/*-------------------------------------------------------------------------
 *
 * pg_hap_encoded_attribute.c
 *	  routines to support manipulation of the pg_hap_encoded_attribute catalog
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/pg_hap_encoded_attribute.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "miscadmin.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "hap/hap_catalog.h"

/*
 * HapInsertEncodedAttribute
 *		Add a new entry to the pg_hap_encoded_attribute.
 *
 * When a new attribute is encoded, it's informations saved to the
 * pg_hap_encoded_attribute.
 *
 * Through the catalog, we know what tables has encoded what attributes and how
 * it can be translated to encoding values.
 */
void
HapInsertEncodedAttribute(Oid haprelid, Oid hapencodetable,
						  int16 hapattrnum, int16 hapdescid, int hapcardinality)
{
	Datum values[Natts_pg_hap_encoded_attribute];	
	bool nulls[Natts_pg_hap_encoded_attribute];
	Relation pghapencodedRel;
	HeapTuple htup;

	/* Open the pg_hap_hidden_attribute_Desc to insert */
	pghapencodedRel = table_open(HapEncodedAttributeRelationId,
							  	 RowExclusiveLock);

	values[Anum_pg_hap_encoded_attribute_haprelid - 1]
		= ObjectIdGetDatum(haprelid);
	values[Anum_pg_hap_encoded_attribute_hapencodetable - 1]
		= ObjectIdGetDatum(hapencodetable);
	values[Anum_pg_hap_encoded_attribute_hapattrnum - 1]
		= Int16GetDatum(hapattrnum);
	values[Anum_pg_hap_encoded_attribute_hapdescid - 1]
		= Int16GetDatum(hapdescid);
	values[Anum_pg_hap_encoded_attribute_hapcardinality - 1]
		= Int32GetDatum(hapcardinality);

	memset(nulls, 0, sizeof(nulls));

	htup = heap_form_tuple(RelationGetDescr(pghapencodedRel), values, nulls);

	/* Insert new pg_hap_encoded_attribute entry */
	CatalogTupleInsert(pghapencodedRel, htup);

	/* Done */
	heap_freetuple(htup);
	table_close(pghapencodedRel, RowExclusiveLock);

	/* Make sure the tuple is visible for subsequent lookups/updates */
	CommandCounterIncrement();
}

/*
 * HapDeleteEncodedAttribute
 *		Delete pg_hap_encoded_attribute tuples with the given relation oid.
 */
void
HapDeleteEncodedAttribute(Oid relid)
{
	Relation pghapencodedRel;
	ScanKeyData scankey;
	SysScanDesc scandesc;
	HeapTuple htup;

	/* Find and delete pg_hap_encoded_attribute entries */
	pghapencodedRel = table_open(HapEncodedAttributeRelationId,
								 RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_pg_hap_encoded_attribute_haprelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	scandesc = systable_beginscan(pghapencodedRel,
								  HapEncodedAttributeRelidAttrnumIndexId,
								  true, NULL, 1,  &scankey);

	while (HeapTupleIsValid(htup = systable_getnext(scandesc)))
		CatalogTupleDelete(pghapencodedRel, &htup->t_self);

	/* Done */
	systable_endscan(scandesc);
	table_close(pghapencodedRel, RowExclusiveLock);
}

/*
 * HapGetEncodeTableId
 *		Using the given relation oid and attribute number, return the encode
 *		table oid.
 *
 * Each encoded attribute has corresponding mapping table. This functon returns
 * the oid of the table through syscache.
 */
Oid
HapGetEncodeTableId(Oid relid, int16 attrnum)
{
	Form_pg_hap_encoded_attribute hapencodedtup;
	HeapTuple tuple;
	Oid  tableid;

	tuple = SearchSysCache2(HAPENCODEDID,
							ObjectIdGetDatum(relid), Int16GetDatum(attrnum));
	if (!HeapTupleIsValid(tuple))
		return InvalidOid;

	hapencodedtup = (Form_pg_hap_encoded_attribute) GETSTRUCT(tuple);
	tableid = hapencodedtup->hapencodetable;
	ReleaseSysCache(tuple);

	return tableid;
}

/*
 * HapGetDimensionHiddenAttrDescid
 *		Using the given relaton oid and attribute number, return the hidden
 *		attribute descriptor id.
 *
 * Note that this function can be used for encoded tables.
 * Otherwise it will return -1.
 */
int16
HapGetDimensionHiddenAttrDescid(Oid relid, int16 attrnum)
{
	Form_pg_hap_encoded_attribute hapencodedtup;
	HeapTuple tuple;
	int16 descid;

	tuple = SearchSysCache2(HAPENCODEDID,
						    ObjectIdGetDatum(relid), Int16GetDatum(attrnum));
	if (!HeapTupleIsValid(tuple))
		return -1;

	hapencodedtup = (Form_pg_hap_encoded_attribute) GETSTRUCT(tuple);
	descid = hapencodedtup->hapdescid;
	ReleaseSysCache(tuple);

	return descid;
}

/*
 * HapGetEncodeTableCardinality
 *		Using the given relation oid and attribute number, return the
 *		cardinality of the encode table.
 *
 * Return the total number of tuples in encode table. Return -1 if there is no
 * encode table for the given arguments.
 */
int
HapGetEncodeTableCardinality(Oid relid, int16 attrnum)
{
	Form_pg_hap_encoded_attribute hapencodedtup;
	HeapTuple tuple;
	int cardinality;

	tuple = SearchSysCache2(HAPENCODEDID,
						    ObjectIdGetDatum(relid), Int16GetDatum(attrnum));
	if (!HeapTupleIsValid(tuple))
		return -1;

	hapencodedtup = (Form_pg_hap_encoded_attribute) GETSTRUCT(tuple);
	cardinality = hapencodedtup->hapcardinality;
	ReleaseSysCache(tuple);

	return cardinality;
}
