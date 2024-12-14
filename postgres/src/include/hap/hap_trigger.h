/*-------------------------------------------------------------------------
 *
 * hap_trigger.h
 *	  POSTGRES hidden attribute partitioning (HAP) triggers.
 *
 *
 * src/include/hap/hap_trigger.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAP_TRIGGER_H
#define HAP_TRIGGER_H

#include "access/tableam.h"
#include "catalog/objectaddress.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"

extern void HapExecForeignKeyCheckTriggers(EState *estate,
										   ResultRelInfo *relinfo,
										   TupleTableSlot *slot);

#endif	/* HAP_TRIGGER_H */
