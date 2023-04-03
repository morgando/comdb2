/*
   Copyright 2018-2020 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include <stddef.h>
#include <stdlib.h>
#include <plhash.h>
#include <bdb/bdb_int.h>

#include "comdb2.h"
#include "sql.h"
#include "build/db.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "types.h"
#include "trancommit_systable.h"

sqlite3_module systblTransactionCommitModule =
{
	.access_flag = CDB2_ALLOW_USER,
};

typedef struct add_tran_commit_args {
	txn_commit_info **data;
	int *npoints;
} add_tran_commit_args;

int add_tran_commit(void *obj, void *arg) {
	UTXNID_TRACK* item = (UTXNID_TRACK*) obj;
	add_tran_commit_args* info = (add_tran_commit_args *) arg;
	info->utxnid = item->utxnid;
	info->commit_lsn_file = item->commit_lsn.file;
	info->commit_lsn_offset = item->commit_lsn.offset;
	printf("utxnid %"PRIx64" lsn file %d offset %d\n", info->utxnid, item->commit_lsn.file, item->commit_lsn.offset);
	++info;
	return 0;
}

int get_tran_commits(void **data, int *npoints) {
	int ret = 0;
	bdb_state_type *bdb_state = thedb->bdb_env;
	Pthread_mutex_lock(&bdb_state->dbenv->mpro->mpro_mutexp);
	*npoints = hash_get_num_entries(bdb_state->dbenv->mpro->transactions);
	void * info;
	*data = info = malloc((*npoints)*sizeof(txn_commit_info));
	printf("1 data %ld\n", (uintptr_t) data);

	ret = hash_for(bdb_state->dbenv->mpro->transactions, add_tran_commit, data);
	Pthread_mutex_unlock(&bdb_state->dbenv->mpro->mpro_mutexp);
	printf("2 data %ld\n", (uintptr_t) data);
	*data = info;
	printf("3 data %ld\n", (uintptr_t) data);
	return ret;
}

void free_tran_commits(void *data, int npoints) {
	bdb_state_type *state_info = data;
	free(state_info);
}

int systblTranCommitInit(sqlite3 *db) {
	return create_system_table(
		db, "comdb2_transaction_commit", &systblTransactionCommitModule,
		get_tran_commits, free_tran_commits, sizeof(txn_commit_info),
		CDB2_INTEGER, "utxnid", -1, offsetof(txn_commit_info, utxnid),
		CDB2_INTEGER, "commitlsnfile", -1, offsetof(txn_commit_info, commit_lsn_file),
		CDB2_INTEGER, "commitlsnoffset", -1, offsetof(txn_commit_info, commit_lsn_offset),
		SYSTABLE_END_OF_FIELDS
	);
}
