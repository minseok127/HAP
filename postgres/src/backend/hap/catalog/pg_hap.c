/*-------------------------------------------------------------------------
 *
 * pg_hap.c
 *	  routines to support manipulation of the pg_hap catalog
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/pg_hap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_hap.h"
#include "storage/lockdefs.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

#include "hap/hap_catalog.h"

/*
 * HapInsertRelTuple
 *		Create a single pg_hap entry for the new relation.
 *
 * When a table using the HAP access method is created, it is registered in
 * pg_hap through this function. 
 */
void
HapInsertRelTuple(Relation rel,
				  int16 hiddenAttrBitSize, int16 hiddenAttrDescCount)
{
	Datum		values[Natts_pg_hap];
	bool		nulls[Natts_pg_hap];
	HeapTuple	htup;
	Relation	pghapRel;
	Oid			relId = RelationGetRelid(rel);
	Oid			relnamespace = RelationGetNamespace(rel);
	char	   *relname = RelationGetRelationName(rel);

	pghapRel = table_open(HapRelationId, RowExclusiveLock);

	/* Make a pg_hap entry */
	values[Anum_pg_hap_haprelid - 1]
		= ObjectIdGetDatum(relId);
	values[Anum_pg_hap_happartmapid - 1]
		= ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_hap_haprelnamespace - 1]
		= ObjectIdGetDatum(relnamespace);
	values[Anum_pg_hap_hapbitsize - 1]
		= Int16GetDatum(hiddenAttrBitSize);
	values[Anum_pg_hap_hapdesccount - 1]
		= Int16GetDatum(hiddenAttrDescCount);
	values[Anum_pg_hap_hapencoded - 1]
		= BoolGetDatum(false);
	values[Anum_pg_hap_haprelname - 1]
		= CStringGetDatum(relname);

	/* There is no nullable column */
	memset(nulls, 0, sizeof(nulls));

	htup = heap_form_tuple(RelationGetDescr(pghapRel), values, nulls);

	CatalogTupleInsert(pghapRel, htup);

	heap_freetuple(htup);

	table_close(pghapRel, RowExclusiveLock);
}

/*
 * HapDeleteRelTuple
 *		Delete a single pg_hap entry for the dropped relation.
 *
 * When a table using the HAP access method is dropped, corresponding pg_hap
 * entry must be removed by this function.
 */
void
HapDeleteRelTuple(Oid relId)
{
	Relation	pghapRel;
	ScanKeyData	scanKey;
	SysScanDesc	scanDesc;
	HeapTuple	htup;

	/* Find and delete the pg_hap entry */
	pghapRel = table_open(HapRelationId, RowExclusiveLock);
	ScanKeyInit(&scanKey,
				Anum_pg_hap_haprelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relId));
	scanDesc = systable_beginscan(pghapRel, HapRelIdIndexId,
								  true, NULL, 1, &scanKey);

	/* relId is uniqure on the pg_hap, so loop is unnecessary */
	if (HeapTupleIsValid(htup = systable_getnext(scanDesc)))
		CatalogTupleDelete(pghapRel, &htup->t_self);

	/* Done */
	systable_endscan(scanDesc);
	table_close(pghapRel, RowExclusiveLock);
}

/*
 * HapRelnameGetHiddenAttrBitsize
 *		Using the given relation name and namespace, look up the hidden
 *		attribute bitsize.
 *
 * Lookup the pg_hap_haprelname_nsp_index using relation name and namespace oid.
 * Return the size of hidden attribute as bit.
 */
int16
HapRelnameGetHiddenAttrBitsize(const char *relname, Oid relnamespace)
{
	HeapTuple	htup;
	ScanKeyData	scanKey[2];
	SysScanDesc	scanDesc;
	Relation	pghapRel;
	int16		ret = 0;

	/* Scan pg_hap */
	pghapRel = table_open(HapRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0],
				Anum_pg_hap_haprelname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(relname));
	ScanKeyInit(&scanKey[1],
				Anum_pg_hap_haprelnamespace,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relnamespace));
	scanDesc = systable_beginscan(pghapRel, HapNameNspIndexId,
								  true, NULL, 2, scanKey);

	htup = systable_getnext(scanDesc);
	if (!HeapTupleIsValid(htup))
		goto done;

	ret = ((Form_pg_hap) GETSTRUCT(htup))->hapbitsize;

done:
	systable_endscan(scanDesc);
	table_close(pghapRel, AccessShareLock);
	return ret;
}

/*
 * HapRelidGetHiddenAttrBitsize
 *		Using the given relation oid, look up the hidden attribute bitsize.
 *
 * Lookup the pg_hap_haprelid_index using relation oid.
 * Return the size of hidden attribute as bit.
 */
int16
HapRelidGetHiddenAttrBitsize(Oid relid)
{
	Form_pg_hap haptup;
	HeapTuple tuple;
	int16 bitsize;

	tuple = SearchSysCache1(HAPRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return 0;

	haptup = (Form_pg_hap) GETSTRUCT(tuple);
	bitsize = haptup->hapbitsize;
	ReleaseSysCache(tuple);

	return bitsize;
}

/*
 * HapRelidGetHiddenAttrBytesize
 *		Using the given relation oid, calculate the byte size of the hidden
 *		attribute.
 *
 * Change the return value of HapRelidGetHiddenAttrBitsize() to the byte size.
 */
int16
HapRelidGetHiddenAttrBytesize(Oid relid)
{
	int16 bitsize = HapRelidGetHiddenAttrBitsize(relid);

	if ((bitsize & 7) == 0)
		return bitsize  >> 3;
	return (bitsize >> 3) + 1;
}

/*
 * HapRelnameGetHiddenAttrDescCount
 *		Using the given relation name and namespace, look up the hidden
 *		attribute count.
 *
 * Lookup the pg_hap_haprelname_nsp_index using relation name and namespace oid.
 * Return the count of the hidden attribute desc.
 */
int16
HapRelnameGetHiddenAttrDescCount(const char *relname, Oid relnamespace)
{
	HeapTuple	htup;
	ScanKeyData	scanKey[2];
	SysScanDesc	scanDesc;
	Relation	pghapRel;
	int16		ret = 0;

	/* Scan pg_hap */
	pghapRel = table_open(HapRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0],
				Anum_pg_hap_haprelname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(relname));
	ScanKeyInit(&scanKey[1],
				Anum_pg_hap_haprelnamespace,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relnamespace));
	scanDesc = systable_beginscan(pghapRel, HapNameNspIndexId,
								  true, NULL, 2, scanKey);

	htup = systable_getnext(scanDesc);
	if (!HeapTupleIsValid(htup))
		goto done;

	ret = ((Form_pg_hap) GETSTRUCT(htup))->hapdesccount;

done:
	systable_endscan(scanDesc);
	table_close(pghapRel, AccessShareLock);
	return ret;
}

/*
 * HapRelidGetHiddenAttrDescCount
 *		Using the given relation oid, look up the hidden attribute count.
 *
 * Lookup the pg_hap_haprelid_index using relation oid.
 * Return the count of the hidden attribute desc.
 */
int16
HapRelidGetHiddenAttrDescCount(Oid relid)
{
	Form_pg_hap haptup;
	HeapTuple tuple;
	int16 desccount;

	tuple = SearchSysCache1(HAPRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return 0;

	haptup = (Form_pg_hap) GETSTRUCT(tuple);
	desccount = haptup->hapdesccount;
	ReleaseSysCache(tuple);

	return desccount;
}

/*
 * HapRelidAddNewHiddenAttrDesc
 *		Using the given relation oid, look up the pg_hap entry and update hidden
 *		attribute information for the new descriptor.
 */
void
HapRelidAddNewHiddenAttrDesc(Oid relid, int16 bitsize)
{
	HeapTuple htup;
	Relation pghapRel;
	Form_pg_hap pghapTup;

	pghapRel = table_open(HapRelationId, RowExclusiveLock);

	htup = SearchSysCacheCopy1(HAPRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for adding hidden attribute descriptor");

	pghapTup = (Form_pg_hap) GETSTRUCT(htup);
	pghapTup->hapdesccount += 1;
	pghapTup->hapbitsize += bitsize;

	CatalogTupleUpdate(pghapRel, &htup->t_self, htup);

	heap_freetuple(htup);

	/* Make sure the tuple is visible for subsequent lookups/updates */
	CommandCounterIncrement();

	table_close(pghapRel, RowExclusiveLock);
}

/*
 * HapRelidAddHiddenAttrDescBulk
 *		Using the given relation oid, look up the pg_hap entry and update hidden
 *		attribute information for the new descriptor.
 */
void
HapRelidAddHiddenAttrDescBulk(Oid relid, int16 desccount, int16 bitsize)
{
	HeapTuple htup;
	Relation pghapRel;
	Form_pg_hap pghapTup;

	pghapRel = table_open(HapRelationId, RowExclusiveLock);

	htup = SearchSysCacheCopy1(HAPRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for adding hidden attribute descriptor");

	pghapTup = (Form_pg_hap) GETSTRUCT(htup);
	pghapTup->hapdesccount += desccount;
	pghapTup->hapbitsize += bitsize;

	CatalogTupleUpdate(pghapRel, &htup->t_self, htup);

	heap_freetuple(htup);

	/* Make sure the tuple is visible for subsequent lookups/updates */
	CommandCounterIncrement();

	table_close(pghapRel, RowExclusiveLock);
}

/*
 * HapRelidMarkAsEncoded
 *		Using the given relation oid, look up the pg_hap entry and update
 *		encoded mark to true.
 */
void
HapRelidMarkAsEncoded(Oid relid)
{
	HeapTuple htup;
	Relation pghapRel;
	Form_pg_hap pghapTup;

	pghapRel = table_open(HapRelationId, RowExclusiveLock);

	htup = SearchSysCacheCopy1(HAPRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for adding hidden attribute descriptor");

	pghapTup = (Form_pg_hap) GETSTRUCT(htup);
	pghapTup->hapencoded = true;

	CatalogTupleUpdate(pghapRel, &htup->t_self, htup);

	heap_freetuple(htup);

	/* Make sure the tuple is visible for subsequent lookups/updates */
	CommandCounterIncrement();

	table_close(pghapRel, RowExclusiveLock);
}

/*
 * HapRelidIsEncoded
 *		Using the given relation oid, look up the pg_hap entry and get whether
 *		this relation is encoded or not.
 */
bool
HapRelidIsEncoded(Oid relid)
{
	Form_pg_hap haptup;
	HeapTuple tuple;
	bool encoded;

	tuple = SearchSysCache1(HAPRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return false;

	haptup = (Form_pg_hap) GETSTRUCT(tuple);
	encoded = haptup->hapencoded;
	ReleaseSysCache(tuple);

	return encoded;
}
