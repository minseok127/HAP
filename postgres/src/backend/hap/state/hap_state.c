/*-------------------------------------------------------------------------
 *
 * hap_state.c
 *	  Execute state for HAP
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/hapstate/hap_state.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/fe_memutils.h"
#include "nodes/execnodes.h"

#include "hap/hap_state.h"

/*
 * Like EState, HAP has its own execution state. Create it.
 */
HapState *
HapCreateState(MemoryContext parent_context)
{
	HapState *hapstate = palloc0(sizeof(HapState));
	MemoryContext oldcontext;

	/* Allocate state data structures in our own memory context */
	hapstate->hap_context = AllocSetContextCreate(parent_context,
												  "HAP Memory Context",
												  ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(hapstate->hap_context);

	/* INSERT state */
	hapstate->hap_insert_state = palloc0(sizeof(HapInsertState));
	hapstate->hap_insert_state->nbyte = -1;

	/* UPDATE state */

	/* DELETE state */

	/* Done */
	MemoryContextSwitchTo(oldcontext);

	return hapstate;
}

/*
 * Delete execution states for HAP.
 */
void
HapDestroyState(HapState *hapstate)
{
	MemoryContextDelete(hapstate->hap_context);
	pfree(hapstate);
}


