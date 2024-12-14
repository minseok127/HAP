/*-------------------------------------------------------------------------
 *
 * hap_state.h
 *	  POSTGRES hidden attribute partitioning (HAP) state.
 *
 *
 * src/include/hap/hap_state.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAP_STATE_H
#define HAP_STATE_H

#include "nodes/pg_list.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

#include "hap_partition.h"

/*
 * HapInsertState - Execution state for INSERT
 *
 * - parent_relid_list: Parent table oid list.
 * - parent_desc_map: Key is index of parent and value is list of descriptor.
 * - child_desc_map: Child descriptors corresponding to parent_desc_map.
 * - encoding_value_map: Encoding values corresponding to parent_desc_map.
 * - relid: The target relation id.
 * - partkeys: Partition keys.
 * - nbyte: Hidden attribute byte size.
 */
typedef struct HapInsertState
{
	List *parent_relid_list;
	List *parent_desc_map;
	List *child_desc_map;
	List *encoding_value_map;
	Datum partkeys[32];
	int partkeynum;
	int nbyte;
} HapInsertState;

/*
 * HapState - Execution state for HAP
 *
 * - hap_context: Memory context for HAP.
 * - hap_insert_state: INSERT state.
 * - hap_update_state: UPDATE state.
 * - hap_delete_staet: DELETE state.
 * - relation_list: Relations that are opend by HAP.
 * - lockmode_list: Lockmodes that are used to open relation_list.
 */
typedef struct HapState
{
	MemoryContext hap_context;
	HapInsertState *hap_insert_state;
	List *relation_list;
	List *lockmode_list;
} HapState;

extern HapState *HapCreateState(MemoryContext parent_context);

extern void HapDestroyState(HapState *hapstate);

#endif	/* HAP_STATE_H */
