#include "build/db.h"
#include "build/db_int.h"
#include "dbinc_auto/os_ext.h"

int __mempro_init(DB_ENV *dbenv) {
	int ret = 0;
	DB_MPRO *mp;
	ret = __os_calloc(dbenv, 1, sizeof(DB_MPRO), &mp);
	if (ret) {
		goto err;
	}

	/* Create transaction map */
	mp->transactions = hash_init_o(offsetof(UTXNID_TRACK, utxnid), sizeof(u_int64_t));
	mp->logfile_lists = hash_init_o(offsetof(LOGFILE_TXN_LIST, file_num), sizeof(u_int32_t));
	if (mp->transactions == NULL) {
		ret = ENOMEM;
		goto err;
	}

	Pthread_mutex_init(&mp->mpro_mutexp, NULL);
	dbenv->mpro = mp;
	return 0;
err:
	logmsg(LOGMSG_ERROR, "Failed to initialize mempro\n");
	return ret;
}

int __mempro_destroy(DB_ENV *env) {
	if (env->mpro) {
		Pthread_mutex_destroy(&env->mpro->mpro_mutexp);
		__os_free(env, env->mpro);
	}
	return 0;
}

int __mempro_remove_txn(DB_ENV *dbenv, u_int64_t utxnid) {
	UTXNID_TRACK *txn;
	int ret = 0;

	printf("Removing txn with id %"PRIx64" from map\n", utxnid);

	Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);
	txn = hash_find(dbenv->mpro->transactions, &utxnid);
	if (txn) {
		hash_del(dbenv->mpro->transactions, txn);
		Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
		free(txn); // TODO: os_free
	} else {
		Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
	}
	return ret;
}

int __mempro_delete_logfile_txns(DB_ENV *dbenv, u_int32_t del_log) {
	int ret = 0;
	UTXNID* elt;
	LOGFILE_TXN_LIST *to_delete = hash_find(dbenv->mpro->logfile_lists, &del_log);

	printf("DELETE LOGFILE TXNS\n");
	if (to_delete) {
		LISTC_FOR_EACH(&to_delete->commit_utxnids, elt, lnk)
		{
			__mempro_remove_txn(dbenv, elt->utxnid);
			free(elt);
		}

		hash_del(dbenv->mpro->logfile_lists, &del_log);
		free(to_delete);
	} else {
		ret = 1;
	}
	return ret;
}

int __mempro_get_commit_lsn_for_txn(DB_ENV *dbenv, u_int64_t utxnid, DB_LSN *commit_lsn) {
	UTXNID_TRACK *txn;
	DB_MPRO *mpro = dbenv->mpro;
	int ret = 0;
	ZERO_LSN(*commit_lsn);

	Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);
	txn = hash_find(dbenv->mpro->transactions, &utxnid);
	if (txn == NULL) {
		ret = DB_NOTFOUND;
	} else {
		*commit_lsn = txn->commit_lsn;
	}
	Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
	return ret;
}

int __mempro_add_txn_commit(DB_ENV *dbenv, u_int64_t utxnid, DB_LSN commit_lsn) {
	int ret = 0;
	if (IS_ZERO_LSN(commit_lsn)) {
		return ret;
	}
	UTXNID_TRACK *txn;
	LOGFILE_TXN_LIST *to_delete = hash_find(dbenv->mpro->logfile_lists, &commit_lsn.file);
	Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);
	txn = hash_find(dbenv->mpro->transactions, &utxnid);
	if (txn) {
		txn->commit_lsn = commit_lsn;
		if (to_delete) {
			UTXNID* elt = (UTXNID*) malloc(sizeof(UTXNID));
			elt->utxnid = utxnid;
			listc_atl(&to_delete->commit_utxnids, elt);
		} else {
			to_delete = (LOGFILE_TXN_LIST*) malloc(sizeof(LOGFILE_TXN_LIST));
			listc_init(&to_delete->commit_utxnids, offsetof(UTXNID, lnk));
			UTXNID *elt = malloc(sizeof(UTXNID));
			elt->utxnid = utxnid;
			listc_atl(&to_delete->commit_utxnids, elt);
			hash_add(dbenv->mpro->logfile_lists, to_delete);
		}
	} else {
		ret = 1;
	}
	Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
	return ret;
}
