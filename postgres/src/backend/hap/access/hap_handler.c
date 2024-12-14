/*-------------------------------------------------------------------------
 *
 * hap_handler.c
 *	  HAP table access method code
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/access/hap_handler.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#include "hap/hap_am.h"

static TableAmRoutine hapam_methods;
static bool hapam_methods_initialized = false;

/*
 * HAP handler function: return TableAmRoutine with access method parameters and
 * callbacks.
 *
 * Note that HAP uses almost the same callback functions as heapam. So copy and
 * use the heapam_method as a HAP method.
 */
Datum
haphandler(PG_FUNCTION_ARGS)
{
	if (!hapam_methods_initialized)
	{
		hapam_methods = *GetHeapamTableAmRoutine();
		hapam_methods_initialized = true;
	}

	PG_RETURN_POINTER(&hapam_methods);
}

/*
 * HapCheckAmUsingName
 *		Check the given name is a name of HAP access method.
 *
 * The OID of the access method may change depending on when the access method
 * is created. But the OID of the handler is immutable.
 *
 * Use this to get the handler's OID from the access method name and check if
 * it's the same as the HAP's handler OID.
 */
bool
HapCheckAmUsingName(const char *accessMethodName)
{
	Form_pg_am pgamForm;
	HeapTuple htup;
	Oid amId, handlerId;

	if (accessMethodName == NULL)
		return false;

	/* Get the handler OID using access method OID */
	amId = get_table_am_oid(accessMethodName, false);
	htup = SearchSysCache1(AMOID, ObjectIdGetDatum(amId));

	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for access method %u", amId);

	pgamForm = (Form_pg_am) GETSTRUCT(htup);
	handlerId = pgamForm->amhandler;

	ReleaseSysCache(htup);

	return (handlerId == HAP_TABLE_ACCESS_METHOD_HANDLER_OID);
}

/*
 * HapCheckAmUsingRelId
 *		Check whether the given oid is in the pg_hap.
 *
 * Return true when the given relId is in the pg_hap. Otherwise return false.
 *
 * Note that we use syscache for fast search and it is released in the
 * SearchSysCacheExists(). So we don't have to call ReleaseSysCache().
 */
bool
HapCheckAmUsingRelId(Oid relId)
{
	return SearchSysCacheExists1(HAPRELID, ObjectIdGetDatum(relId));
}

/*
 * HapCheckAmUsingRel
 *		Check whether the given Relation uses HAP.
 *
 * Return true when the given relId uses HAP access method. Otherwise return
 * false.
 */ 
bool
HapCheckAmUsingRel(Relation rel)
{
#ifdef HAP
	return (rel->rd_is_hap);
#else /* !HAP */
	return false;
#endif /* HAP */ 
}
