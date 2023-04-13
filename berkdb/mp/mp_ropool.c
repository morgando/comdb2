#include "build/db.h"
#include "build/db_int.h"
#include "dbinc_auto/os_ext.h"

/*
 * __mempro_init --
 * 	Initialize read-only memory pool.
 *
 * PUBLIC: int __mempro_init
 * PUBLIC:     __P((DB_ENV *));
 */
int __mempro_init(dbenv) 
	DB_ENV *dbenv;
{
	int ret;
	DB_MPRO *mp;

	ret = __os_calloc(dbenv, 1, sizeof(DB_MPRO), &mp);

	if (ret) {
		goto err;
	}

	mp->transactions = hash_init_o(offsetof(UTXNID_TRACK, utxnid), sizeof(u_int64_t));
	mp->logfile_lists = hash_init_o(offsetof(LOGFILE_TXN_LIST, file_num), sizeof(u_int32_t));

	if (mp->transactions == NULL || mp->logfile_lists == NULL) {
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

/*
 * __mempro_destroy --
 * 	Destroy read-only memory pool.
 *
 * PUBLIC: int __mempro_destroy
 * PUBLIC:     __P((DB_ENV *));
 */
int __mempro_destroy(dbenv) 
	DB_ENV *dbenv;
{
	if (dbenv->mpro) {
		Pthread_mutex_destroy(&dbenv->mpro->mpro_mutexp);
		__os_free(dbenv, dbenv->mpro);
	}

	return 0;
}

/*
 * __mempro_remove_txn --
 *  Remove a transaction from the commit LSN map.	
 *
 * PUBLIC: int __mempro_remove_txn
 * PUBLIC:     __P((DB_ENV *, u_int64_t));
 */
int __mempro_remove_txn(dbenv, utxnid) 
	DB_ENV *dbenv;
	u_int64_t utxnid;
{
	UTXNID_TRACK *txn;
	int ret;

	ret = 0;

	Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);
	txn = hash_find(dbenv->mpro->transactions, &utxnid);

	if (txn) {
		hash_del(dbenv->mpro->transactions, txn);
		__os_free(dbenv, txn); 
	} else {
		ret = 1;
	}

	Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
	return ret;
}

/*
 * __mempro_delete_logfile_txns --
 *  Remove all transactions that committed in a specific logfile 
 *  from the commit LSN map.	
 *
 * PUBLIC: int __mempro_delete_logfile_txns
 * PUBLIC:     __P((DB_ENV *, u_int32_t));
 */
int __mempro_delete_logfile_txns(dbenv, del_log) 
	DB_ENV *dbenv;
	u_int32_t del_log;
{
	LOGFILE_TXN_LIST *cleanup_list;
	UTXNID* elt;
	int ret;

	cleanup_list = hash_find(dbenv->mpro->logfile_lists, &del_log);
	ret = 0;

	Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);

	if (cleanup_list) {
		LISTC_FOR_EACH(&cleanup_list->commit_utxnids, elt, lnk)
		{
			__mempro_remove_txn(dbenv, elt->utxnid);
			__os_free(dbenv, elt);
		}

		hash_del(dbenv->mpro->logfile_lists, &del_log);
		__os_free(dbenv, cleanup_list);
	} else {
		ret = 1;
	}

	Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
	return ret;
}
 
/*
 * __mempro_get_commit_lsn_for_txn --
 *  Get the commit LSN of a transaction.
 *
 * PUBLIC: int __mempro_get_commit_lsn_for_txn
 * PUBLIC:     __P((DB_ENV *, u_int64_t, DB_LSN));
 */
int __mempro_get_commit_lsn_for_txn(DB_ENV *, u_int64_t, DB_LSN *) 
	DB_ENV *dbenv;
	u_int64_t utxnid;
	DB_LSN *commit_lsn;
{
	UTXNID_TRACK *txn;
	DB_MPRO *mpro;
	int ret;
   
	mpro = dbenv->mpro;
	ret = 0;

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

/*
 * __mempro_add_txn_commit --
 *  Store the commit LSN of a transaction.
 *
 * PUBLIC: int __mempro_add_txn_commit
 * PUBLIC:     __P((DB_ENV *, u_int64_t, DB_LSN));
 */
int __mempro_add_txn_commit(DB_ENV *dbenv, u_int64_t utxnid, DB_LSN commit_lsn) {
	LOGFILE_TXN_LIST *cleanup_list
	UTXNID_TRACK *txn;
	UTXNID* elt;

	/* Don't add transactions that commit at the zero LSN (this is not an error) */
	if (IS_ZERO_LSN(commit_lsn)) {
		return 0;
	}

	if (__os_malloc(dbenv, sizeof(UTXNID_TRACK), txn) != 0) {
		ret = ENOMEM;
		goto err;
	}

	txn->commit_lsn = commit_lsn;

	Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);
	cleanup_list = hash_find(dbenv->mpro->logfile_lists, &commit_lsn.file);

	if (!cleanup_list) {
		if (__os_malloc(dbenv, sizeof(LOGFILE_TXN_LIST), cleanup_list) != 0) {
			Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
			return ENOMEM;
		}

		cleanup_list->file_num = commit_lsn.file;
		listc_init(&cleanup_list->commit_utxnids, offsetof(UTXNID, lnk));
		hash_add(dbenv->mpro->logfile_lists, cleanup_list);
	}

	if (__os_malloc(dbenv, sizeof(UTXNID), elt) != 0) {
		Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
		return ENOMEM;
	}

	/* Add transaction to the cleanup list for the log file of its commit */
	elt->utxnid = utxnid;
	listc_atl(&cleanup_list->commit_utxnids, elt);
	return 0;
}
