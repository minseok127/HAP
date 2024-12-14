/*-------------------------------------------------------------------------
 *
 * hap_executor.h
 *	  POSTGRES light internal executor for HAP.
 *
 *
 * src/include/hap/hap_executor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAP_EXECUTOR_H
#define HAP_EXECUTOR_H

#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "executor/executor.h"

EState *HapInitLightWeightExecutor(List *relid_list, List *lockmode_list);

void HapDoneLightWeightExecutor(EState *estate, PlanState *ps);

#endif /* HAP_EXECUTOR_H */
