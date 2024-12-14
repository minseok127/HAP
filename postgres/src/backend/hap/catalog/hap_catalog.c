/*-------------------------------------------------------------------------
 *
 * hap_catalog.c
 *	  routines to support HAP related catalogs.
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/hap_catalog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_constraint.h"
#include "miscadmin.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/array.h"

#include "hap/hap_catalog.h"

/*
 * HapDeleteCatalogTuples
 *		Remove HAP related catalog tuples.
 *
 * When a table is dropped, all related HAP catalog entries are deleted by this
 * function.
 */
void
HapDeleteCatalogTuples(Oid relId)
{
	/* pg_hap */
	HapDeleteRelTuple(relId);

	/* pg_hap_hidden_attribute_desc */
	HapDeleteHiddenAttrDesc(relId);

	/* pg_hap_encoded_attribute */
	HapDeleteEncodedAttribute(relId);
}

/*
 * HapGetReferencingRelids
 * 		Lookup the pg_constraint and gather the referncing table oids.
 *
 * Among the existing pg_constraint relationd functions, there is no function
 * that returns the oid of the referencing tables.
 *
 * Since HAP requires this function, it is implemented by HAP module.
 */
List *
HapGetReferencingRelids(Oid confrelid)
{
	SysScanDesc	scandesc;
	ScanKeyData	scankey;
	List *result = NIL;
	Relation	conrel;
	HeapTuple	htup;

	ScanKeyInit(&scankey,
				Anum_pg_constraint_confrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(confrelid));
	conrel = table_open(ConstraintRelationId, AccessShareLock);

	/* pg_constraint has no index for confrelid. So use sequential scan */
	scandesc = systable_beginscan(conrel, InvalidOid, false, NULL, 1, &scankey);

	while (HeapTupleIsValid(htup = systable_getnext(scandesc)))
	{
		Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(htup);

		/* Consider only foreign key constraint */
		if (constraint->contype != CONSTRAINT_FOREIGN)
			continue;

		Assert(constraint->confrelid == confrelid);
		result = lappend_oid(result, constraint->conrelid);
	}

	/* Done */
	systable_endscan(scandesc);
	table_close(conrel, AccessShareLock);

	return result;
}

/*
 * HapDeconstructFKeyConst
 * 		Analyze foreign key relationship between the given tables.
 *
 * In @confkeys and @conkeys, the foreign key attribute numbers are entered.
 * The return value is the number of keys.
 */
int
HapDeconstructFKeyConst(Oid confrelid, Oid conrelid,
						int16 *confkeys, int16 *conkeys)
{
	ScanKeyData scankey[2];
	SysScanDesc scandesc;
	Relation conrel;
	HeapTuple htup;
	Datum arrdatum;
	ArrayType *arr;
	int numkeys;
	bool isNull;

	/* Prepare to scan pg_constraint */
	ScanKeyInit(&scankey[0],
				Anum_pg_constraint_confrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(confrelid));
	ScanKeyInit(&scankey[1],
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(conrelid));
	conrel = table_open(ConstraintRelationId, AccessShareLock);

	/* pg_constraint has no index for confrelid. So use sequential scan */
	scandesc = systable_beginscan(conrel, InvalidOid, false, NULL, 2, scankey);

	while (HeapTupleIsValid(htup = systable_getnext(scandesc)))
	{
		Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(htup);

		/* Consider only foreign key constraint */
		if (constraint->contype == CONSTRAINT_FOREIGN)
			break;
	}

	Assert(htup != NULL);

	/* Get the key array of the referencing table using syscache */
	arrdatum = SysCacheGetAttr(CONSTROID, htup,
							   Anum_pg_constraint_conkey, &isNull);
	arr = DatumGetArrayTypeP(arrdatum);
	numkeys = ARR_DIMS(arr)[0];
	memcpy(conkeys, ARR_DATA_PTR(arr), numkeys * sizeof(int16));
	if ((Pointer) arr != DatumGetPointer(arrdatum))
		pfree(arr); /* free de-toasted copy, if any */

	/* Get the key array of the referenced table using syscache */
	arrdatum = SysCacheGetAttr(CONSTROID, htup,
							   Anum_pg_constraint_confkey, &isNull);
	arr = DatumGetArrayTypeP(arrdatum);
	numkeys = ARR_DIMS(arr)[0];
	memcpy(confkeys, ARR_DATA_PTR(arr), numkeys * sizeof(int16));
	if ((Pointer) arr != DatumGetPointer(arrdatum))
		pfree(arr); /* free de-toasted copy, if any */

	/* Done */
	systable_endscan(scandesc);
	table_close(conrel, AccessShareLock);

	return numkeys;
}
