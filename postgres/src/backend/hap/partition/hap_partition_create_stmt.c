/*-------------------------------------------------------------------------
 *
 * hap_partition_create_stmt.c
 *	  Create statement for creating hidden partitions
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/planner/hap_partition_create_stmt.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/lsyscache.h"
#include "catalog/pg_type.h"

#include "hap/hap_partition.h"

/*
 * HapSetHiddenPartKey
 *		Translate hidden partition key option.
 *
 * The hidden partition key option has a string. Interpret this to figure out
 * which hidden attributes are used as partition keys.
 *
 * Then read the dimenstion tables to figure out the possible combinations of
 * the hidden attributes.
 */
static void
HapSetHiddenPartKey(HapPartCreateStmt *stmt, DefElem *hidden_partition_key)
{
	char *keystr;

	if (!IsA(hidden_partition_key->arg, String))
		elog(ERROR, "Hidden partition key option's type must be a string");

	keystr = ((String *) hidden_partition_key->arg)->sval;

	HapParsePartKeyStr(stmt, keystr);

	HapFindPartKeyValues(stmt);
}

/*
 * HapSetHiddenPartNum
 *		Translate hidden partition num option.
 *
 * This option represents maximum number of partitions.
 *
 * If this option is NULL, there is no limit on the number of partition to be
 * created.
 */
static void
HapSetHiddenPartNum(HapPartCreateStmt *stmt, DefElem *hidden_partition_num)
{
	int part_count;

	/* One partition per set of partition keys */
	if (hidden_partition_num == NULL)
	{
		stmt->part_count = stmt->total_value_set_count;
		stmt->value_set_count_per_part = 1;
		return;
	}

	if (!IsA(hidden_partition_num->arg, Integer))
		elog(ERROR, "Hidden partition num option's type must be a int");

	part_count = ((Integer *) hidden_partition_num->arg)->ival;

	if (part_count >= stmt->total_value_set_count)
	{
		stmt->part_count = stmt->total_value_set_count;
		stmt->value_set_count_per_part = 1;
	}
	else
	{
		stmt->part_count = part_count;
		stmt->value_set_count_per_part
			= stmt->total_value_set_count / part_count;
	}
}

/*
 * HapBuildPartCreateStmt
 *		Translate the given relopts and build partitioning statement.
 *
 * Find possible hidden attribute combination list based on hidden_partition_key
 * and form groups based on hidden_partition_num. Each group corresponds to each
 * partition.
 *
 * Note that hidden_partition_num is optional. If this option is not available,
 * there is no limit on the number of partitions to be created (one partition
 * per hidden attribute combination).
 */
void HapBuildPartCreateStmt(HapPartCreateStmt *stmt, Oid relid,
							DefElem *hidden_partition_key,
							DefElem *hidden_partition_num)
{
	/* Initialize the statement */
	stmt->target_relid = relid;
	stmt->partkey_list = NIL;
	stmt->partkey_set_list = NIL;
	stmt->part_count = 0;
	stmt->total_value_set_count = 1;
	stmt->value_set_count_per_part = 1;

	Assert(hidden_partition_key != NULL);

	HapSetHiddenPartKey(stmt, hidden_partition_key);

	HapSetHiddenPartNum(stmt, hidden_partition_num);
}
