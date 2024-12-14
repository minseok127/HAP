/*-------------------------------------------------------------------------
 *
 * pg_hap_hidden_attribute_desc.c
 *	  routines to support manipulation of the pg_hap_hidden_attribute_desc catalog
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/pg_hap_hidden_attribute_desc.c
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

#include "hap/hap_am.h"
#include "hap/hap_catalog.h"

/*
 * Insert function for pg_hap_hidden_attribute_desc catalog.
 */
static void
__HapInsertHiddenAttrDesc(Oid haprelid, Oid hapconfrelid,
						  int16 hapstartbit, int16 hapbitsize,
						  int16 hapdescid, int16 hapconfdescid)
{
	Datum values[Natts_pg_hap_hidden_attribute_desc];	
	bool nulls[Natts_pg_hap_hidden_attribute_desc];
	Relation pghapdescRel;
	HeapTuple htup;

	/* Open the pg_hap_hidden_attribute_Desc to insert */
	pghapdescRel = table_open(HapHiddenAttributeDescRelationId,
							  RowExclusiveLock);

	values[Anum_pg_hap_hidden_attribute_desc_haprelid - 1]
		= ObjectIdGetDatum(haprelid);
	values[Anum_pg_hap_hidden_attribute_desc_hapconfrelid - 1]
		= ObjectIdGetDatum(hapconfrelid);
	values[Anum_pg_hap_hidden_attribute_desc_hapstartbit - 1]
		= Int16GetDatum(hapstartbit);
	values[Anum_pg_hap_hidden_attribute_desc_hapbitsize - 1]
		= Int16GetDatum(hapbitsize);
	values[Anum_pg_hap_hidden_attribute_desc_hapdescid - 1]
		= Int16GetDatum(hapdescid);
	values[Anum_pg_hap_hidden_attribute_desc_hapconfdescid - 1]
		= Int16GetDatum(hapconfdescid);
	values[Anum_pg_hap_hidden_attribute_desc_happartkeyidx - 1]
		= Int16GetDatum(-1);

	memset(nulls, 0, sizeof(nulls));

	htup = heap_form_tuple(RelationGetDescr(pghapdescRel), values, nulls);

	/* Insert new pg_hap_hidden_attribute_desc entry */
	CatalogTupleInsert(pghapdescRel, htup);

	/* Done */
	heap_freetuple(htup);
	table_close(pghapdescRel, RowExclusiveLock);

	/* Make sure the tuple is visible for subsequent lookups/updates */
	CommandCounterIncrement();
}

/*
 * HapPrepareNewHiddenAttrDesc
 *		Prepare to put a new entry in pg_hap_hidden_attribute_desc catalog.
 *
 * Analyze the pg_hap catalog to get the information the new hidden attribute
 * descriptor will have.
 *
 * In this process, also perform pg_hap udpates that will occur due to the new
 * hidden attribute descriptor.
 */
static void
HapPrepareNewHiddenAttrDesc(Oid relid, int16 bitsize,
							int16 *startbit, int16 *descid)
{
	/*
	 * New hidden attribute descriptor's start bit is same with current hidden
	 * attribute bit size of the given table.
	 */
	*startbit = HapRelidGetHiddenAttrBitsize(relid);

	/*
	 * New descriptor's id is same with current hidden attribute descriptor
	 * count of the given table.
	 */
	*descid = HapRelidGetHiddenAttrDescCount(relid);

	/* Add a new hidden attribute descriptor information to the pg_hap entry */
	HapRelidAddNewHiddenAttrDesc(relid, bitsize);
}

/*
 * HapPropagateHiddenAttrDesc
 *		Propagate the new hidden attribute descriptor to the referencing tables.
 *
 * Propagate the new hidden attribute descriptor. This function is called
 * recursively through foreign key relationships.
 */
static void
HapPropagateHiddenAttrDesc(Oid confrelid, int16 confdescid, int16 bitsize)
{
	List *conrelOids = NIL;
	ListCell *lc = NULL;

	/* Since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	/* Get the relation oids that are referencing confrelid */
	conrelOids = HapGetReferencingRelids(confrelid);

	/* Propagate the new hidden attribute descriptor recursively */
	foreach(lc, conrelOids)
	{
		int16 startbit, descid;
		Oid conrelid = lfirst_oid(lc);

		/* Ignore non-hap table */
		if (!HapCheckAmUsingRelId(conrelid))
			continue;

		/* Prepare to insert new hidden attribute descriptor */
		HapPrepareNewHiddenAttrDesc(conrelid, bitsize, &startbit, &descid);

		/* Insert pg_hap_hidden_attribute_desc entry for this table */
		__HapInsertHiddenAttrDesc(conrelid, confrelid,
								  startbit, bitsize,
								  descid, confdescid);

		/* Propagate this new descriptor to the referencing tables */
		HapPropagateHiddenAttrDesc(conrelid, descid, bitsize);
	}
}					  

/*
 * HapInsertHiddenAttrDesc
 *		Create a new hidden attribute descriptor for the given relation.
 *
 * Using the given dimension table, create a new hidden attribute descriptor and
 * insert the entry to the pg_hap_hidden_attribute_desc catalog.
 *
 * After that, propagate the descriptor to the referencing tables using foreign
 * key relationship.
 *
 * Return the top hidden attribute descriptor id which means dimension table's
 * descriptor id.
 */
int16
HapInsertHiddenAttrDesc(Oid dimrelid, int16 bitsize)
{
	int16 startbit, descid;

	/* Prepare to insert new hidden attribute descriptor */
	HapPrepareNewHiddenAttrDesc(dimrelid, bitsize, &startbit, &descid);

	/* Insert pg_hap_hidden_attribute_desc entry for this table */
	__HapInsertHiddenAttrDesc(dimrelid, dimrelid, startbit,
							  bitsize, descid, descid);

	/* Propagate this new descriptor to the referencing tables */
	HapPropagateHiddenAttrDesc(dimrelid, descid, bitsize);

	return descid;
}

/*
 * HapDeleteHiddenAttrDesc
 *		Delete pg_hap_hidden_attribute_desc tuples with the given relation oid.
 */
void
HapDeleteHiddenAttrDesc(Oid relid)
{
	Relation	pghapdescRel;
	ScanKeyData	scankey;
	SysScanDesc	scandesc;
	HeapTuple	htup;

	/* Find and delete pg_hap_hidden_attribute_desc entries */
	pghapdescRel = table_open(HapHiddenAttributeDescRelationId,
							  RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_pg_hap_hidden_attribute_desc_haprelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	scandesc = systable_beginscan(pghapdescRel,
								  HapHiddenAttributeDescRelidDescidIndexId,
								  true, NULL, 1, &scankey);

	while (HeapTupleIsValid(htup = systable_getnext(scandesc)))
		CatalogTupleDelete(pghapdescRel, &htup->t_self);

	/* Done */
	systable_endscan(scandesc);
	table_close(pghapdescRel, RowExclusiveLock);
}

/*
 * HapInheritHiddenAttrDesc
 *		Inherit all hidden attribute descriptors of the referenced table.
 */
void
HapInheritHiddenAttrDesc(Oid relid, Oid refrelid)
{
	int16 descid, startbit, totalbitsize = 0;
	Relation pghapdescRel;
	SysScanDesc	scandesc;
	ScanKeyData	scankey;
	HeapTuple htup;

	/* Open pg_hap_hidden_attribute_desc */
	pghapdescRel = table_open(HapHiddenAttributeDescRelationId,
							  AccessShareLock);

	ScanKeyInit(&scankey,
				Anum_pg_hap_hidden_attribute_desc_haprelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(refrelid));
	scandesc = systable_beginscan(pghapdescRel,
								  HapHiddenAttributeDescRelidDescidIndexId,
								  true, NULL, 1, &scankey);

	/* Get the start descriptor id and bit index */
	descid = HapRelidGetHiddenAttrDescCount(relid);
	startbit = HapRelidGetHiddenAttrBitsize(relid);

	/* Inherits referenced table's hidden attribute descriptors */
	while (HeapTupleIsValid(htup = systable_getnext(scandesc)))
	{
		Form_pg_hap_hidden_attribute_desc pghapdescTup =
			(Form_pg_hap_hidden_attribute_desc) GETSTRUCT(htup);
		int16 bitsize, confdescid;

		bitsize = pghapdescTup->hapbitsize;
		confdescid = pghapdescTup->hapdescid;

		/* Insert new entry */
		__HapInsertHiddenAttrDesc(relid, refrelid, startbit, bitsize,
								  descid, confdescid);

		/* Increase startbit and descriptor id for the next */
		startbit += bitsize;
		descid++;
		totalbitsize += bitsize;
	}

	/* Done */
	systable_endscan(scandesc);
	table_close(pghapdescRel, AccessShareLock);

	/* Give this change to the pg_hap too */
	if (totalbitsize)
		HapRelidAddHiddenAttrDescBulk(relid, descid, totalbitsize);
}

/*
 * HapGetChildHiddenAttrDescid
 *		Using the given informations, lookup syscache and get the child hidden
 *		attribute descriptor's id.
 *
 * Hidden Attribute descriptors are linked through a foreign key relationship.
 *
 * This function returns the hidden attribute descriptor id of the child table
 * linked to the parent table's hidden attribute descriptor id.
 */
int16
HapGetChildHiddenAttrDescid(Oid relid, Oid confrelid, int16 confdescid)
{
	Form_pg_hap_hidden_attribute_desc hapdesctup;
	HeapTuple tuple;
	int16 descid;

	tuple = SearchSysCache3(HAPDESCCONFID,
							ObjectIdGetDatum(relid),
							ObjectIdGetDatum(confrelid),
							Int16GetDatum(confdescid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for hidden attribute descriptor");

	hapdesctup = (Form_pg_hap_hidden_attribute_desc) GETSTRUCT(tuple);
	descid = hapdesctup->hapdescid;
	ReleaseSysCache(tuple);

	return descid;
}

/*
 * HapGetDescendantHiddenAttrDescid
 *		From ancestor descriptor id, get descendant descriptor id.
 *
 * In rel_oid_list, if the backward is true, the first element is the relation
 * oid of the descendant to be targeted, and the last element is the relation
 * oid of ancestor.
 *
 * Through this list and the ancestor's hidden attribute descriptor id, the
 * child descriptor id is sequentially identified, and then the desired
 * descendant's descriptor id is returend.
 *
 * Note that the referenced side in the foreign key relationship is the parent
 * table and referencing side is the child table.
 */
int16
HapGetDescendantHiddenAttrDescid(List *rel_oid_list,
								 int16 ancestor_descid, bool backward)
{
	int length = list_length(rel_oid_list);
	int16 descendant_descid;

	Assert(length > 0);

	if (backward)
	{
		for (int parentidx = length - 1; parentidx >= 1; parentidx--)
		{
			Oid parentid = list_nth_oid(rel_oid_list, parentidx);
			Oid childid = list_nth_oid(rel_oid_list, parentidx - 1);

			descendant_descid = HapGetChildHiddenAttrDescid(childid, parentid,
															ancestor_descid);
			ancestor_descid = descendant_descid;
		}
	}
	else
	{
		for (int parentidx = 0; parentidx <= length - 2; parentidx++)
		{
			Oid parentid = list_nth_oid(rel_oid_list, parentidx);
			Oid childid = list_nth_oid(rel_oid_list, parentidx + 1);

			descendant_descid = HapGetChildHiddenAttrDescid(childid, parentid,
															ancestor_descid);
			ancestor_descid = descendant_descid;
		}
	}

	return descendant_descid;
}
