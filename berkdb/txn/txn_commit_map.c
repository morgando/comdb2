#define __commit_map_format(msg, ...) \
	"[COMMIT MAP] %s: " msg, __func__, ## __VA_ARGS

#define __commit_map_debug(commit_map_debug, lvl, msg, ...) \
	if (commit_map_debug) { logmsg(LOGMSG_DEBUG, __commit_map_debug_format(msg, ## __VA_ARGS)) }

static int free_logfile_list_elt(obj, arg)
	void *obj;
	void *arg;
{
	LOGFILE_TXN_LIST * const to_delete = (LOGFILE_TXN_LIST * const) obj;
	DB_ENV * const dbenv = (DB_ENV * const) arg;

	hash_clear(to_delete->commit_utxnids);
	hash_free(to_delete->commit_utxnids);
	__os_free(dbenv, to_delete);
	return 0;
}

static int free_transactions(obj, arg)
	void *obj;
	void *arg;
{
	UTXNID_TRACK *elt;
	DB_ENV *dbenv;

	elt = (UTXNID_TRACK *) obj;
	dbenv = (DB_ENV *) arg;

	__os_free(dbenv, elt);
	return 0;
}

/*
 * Deletes a logfile list from the commit LSN map.
 */
static void __txn_commit_map_delete_logfile_list(DB_ENV *dbenv, LOGFILE_TXN_LIST * const to_delete) {
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;
	const u_int32_t del_log = to_delete->file_num;
	const int i_am_highest_logfile = del_log == txmap->highest_logfile;
	const int i_am_smallest_logfile = del_log == txmap->smallest_logfile;

	if (i_am_highest_logfile && i_am_smallest_logfile)
	{
		logmsg(LOGMSG_WARN, "%s: Deleting the only logfile (%"PRIu32") in txmap\n", __func__, del_log);

		txmap->highest_logfile = -1;
		txmap->smallest_logfile = -1;
	} else if (i_am_highest_logfile)
	{
		logmsg(LOGMSG_WARN, "%s: Deleting the highest logfile (%"PRIu32") in txmap\n", __func__, del_log);

		LOGFILE_TXN_LIST *successor = NULL;
		while ((successor == NULL) && (--txmap->highest_logfile >= 0)) {
			successor = hash_find(txmap->logfile_lists, &txmap->smallest_logfile);
		}
		assert(successor);
	} else if (i_am_smallest_logfile)
	{
		LOGFILE_TXN_LIST *successor = NULL;
		while ((successor == NULL) && (++txmap->smallest_logfile <= txmap->modsnap_start_lsn.file)) {
			successor = hash_find(txmap->logfile_lists, &txmap->smallest_logfile);
		}
		assert(successor);
	}

	hash_del(txmap->logfile_lists, to_delete);
	hash_clear(to_delete->commit_utxnids);
	hash_free(to_delete->commit_utxnids);
	__os_free(dbenv, to_delete);
}

/*
 *  Remove a transaction from the commit LSN map without locking.
 */
static int __txn_commit_map_remove_nolock(dbenv, utxnid, delete_from_logfile_lists)
	DB_ENV *dbenv;
	u_int64_t utxnid;
	int delete_from_logfile_lists;
{
	__commit_map_debug(dbenv->attr.commit_map_debug, "Deleting utxnid %"PRIu64"\n", utxnid);

	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;

	UTXNID_TRACK * const txn = hash_find(txmap->transactions, &utxnid);
	if (!txn) {
		logmsg(LOGMSG_ERROR, "%s: Could not find transaction %"PRIu64" in the map\n", __func__, utxnid);
		return 1;
	}

	LOGFILE_TXN_LIST * const logfile_list = hash_find(txmap->logfile_lists, &(txn->commit_lsn.file));
	if (!logfile_list) {
		logmsg(LOGMSG_ERROR, "%s: Could not find logfile list for file %d\n", __func__, txn->commit_lsn.file);
		return 1;
	}

	if (delete_from_logfile_lists) {
		hash_del(logfile_list->commit_utxnids, txn);
	}

	if (hash_get_num_entries(logfile_list->commit_utxnids) == 0) {
		__txn_commit_map_delete_logfile_list(dbenv, logfile_list);
	}

	hash_del(txmap->transactions, txn);
	__os_free(dbenv, txn); 

	return 0;
}

static int __txn_commit_map_remove_nolock_foreach_wrapper(void *obj, void *arg) {
	return __txn_commit_map_remove_nolock((DB_ENV *) arg, ((UTXNID_TRACK *) obj)->utxnid, 0);
}

int __txn_commit_map_init(dbenv) 
	DB_ENV *dbenv;
{
	int ret;
	DB_TXN_COMMIT_MAP *txmap;

	ret = __os_calloc(dbenv, 1, sizeof(DB_TXN_COMMIT_MAP), &txmap);
	if (ret) {
		goto err;
	}

	txmap->transactions = hash_init_o(offsetof(UTXNID_TRACK, utxnid), sizeof(u_int64_t));
	if (!txmap->transactions) {
		ret = ENOMEM;
		goto err;
	}

	txmap->logfile_lists = hash_init_o(offsetof(LOGFILE_TXN_LIST, file_num), sizeof(u_int32_t));
	if (!txmap->logfile_lists) {
		ret = ENOMEM;
		goto err;
	}

	ZERO_LSN(txmap->highest_checkpoint_lsn);
	ZERO_LSN(txmap->modsnap_start_lsn);
	txmap->smallest_logfile = -1;
	txmap->highest_logfile = -1;
	Pthread_mutex_init(&txmap->txmap_mutexp, NULL);
	dbenv->txmap = txmap;

	return ret;
err:
	if (txmap) {
		if (txmap->transactions) {
			hash_free(txmap->transactions);
		}
		if (txmap->logfile_lists) {
			hash_free(txmap->logfile_lists);
		}
		free(txmap);
	}

	logmsg(LOGMSG_ERROR, "Failed to initialize commit LSN map\n");
	return ret;
}

int __txn_commit_map_destroy(dbenv)
	DB_ENV *dbenv;
{
	if (dbenv->txmap) {
		hash_for(dbenv->txmap->transactions, &free_transactions, (void *) dbenv);
		hash_clear(dbenv->txmap->transactions);
		hash_free(dbenv->txmap->transactions);

		hash_for(dbenv->txmap->logfile_lists, &free_logfile_list_elt, (void *) dbenv);
		hash_clear(dbenv->txmap->logfile_lists);
		hash_free(dbenv->txmap->logfile_lists);

		Pthread_mutex_destroy(&dbenv->txmap->txmap_mutexp);
		__os_free(dbenv, dbenv->txmap);
	}

	return 0;
}

int __txn_commit_map_remove(const DB_ENV * const dbenv, const u_int64_t utxnid)
{
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;
	Pthread_mutex_lock(&txmap->txmap_mutexp);
	const int ret = __txn_commit_map_remove_nolock(dbenv, utxnid, 1);
	Pthread_mutex_unlock(&txmap->txmap_mutexp);
	return ret;
}

void __txn_commit_map_get_highest_checkpoint_lsn(const DB_ENV * const dbenv, DB_LSN * const highest_checkpoint_lsn, const int should_lock)
{
	const DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;
	if (should_lock) { Pthread_mutex_lock(&txmap->txmap_mutexp); }
	*highest_checkpoint_lsn = txmap->highest_checkpoint_lsn;
	if (should_lock) { Pthread_mutex_unlock(&txmap->txmap_mutexp); }
}

void __txn_commit_map_get_modsnap_start_lsn(DB_ENV * const dbenv, DB_LSN * const modsnap_start_lsn, const int should_lock)
{
	const DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;
	if (should_lock) { Pthread_mutex_lock(&txmap->txmap_mutexp); }
	*modsnap_start_lsn = txmap->modsnap_start_lsn;
	if (should_lock) { Pthread_mutex_unlock(&txmap->txmap_mutexp); }
}

void __txn_commit_map_print_info(const DB_ENV * const dbenv, const loglvl lvl, const int should_lock) {
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;

	if (should_lock) { Pthread_mutex_lock(&txmap->txmap_mutexp); }

	logmsg(lvl, "Modsnap start lsn file: %"PRIu32"; "
					"Modsnap start lsn offset: %"PRIu32"; "
					"Highest checkpoint lsn file: %"PRIu32"; "
					"Highest checkpoint lsn offset: %"PRIu32"; "
					"Highest logfile: %"PRId64"; "
					"Smallest logfile: %"PRId64"\n",
					txmap->modsnap_start_lsn.file,
					txmap->modsnap_start_lsn.offset,
					txmap->highest_checkpoint_lsn.file,
					txmap->highest_checkpoint_lsn.offset,
					txmap->highest_logfile,
					txmap->smallest_logfile);

	if (should_lock) { Pthread_mutex_unlock(&txmap->txmap_mutexp); }
}

int __txn_commit_map_delete_logfile_txns(const DB_ENV * const dbenv, const int logfile) 
{
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;
	int ret = 0;

	__commit_map_debug(dbenv->attr.commit_map_debug, "Deleting log %"PRIu32"\n", del_log);

	Pthread_mutex_lock(&txmap->txmap_mutexp);

	LOGFILE_TXN_LIST * const to_delete = hash_find(txmap->logfile_lists, &del_log);
	if (!to_delete) {
		ret = 1;
		goto err;
	}

	ret = hash_for(to_delete->commit_utxnids, (hashforfunc_t *const) __txn_commit_map_remove_nolock_foreach_wrapper, (void *) dbenv);
	if (ret) {
		goto err;
	}

	__txn_commit_map_delete_logfile_list(dbenv, to_delete);
err:
	Pthread_mutex_unlock(&txmap->txmap_mutexp);

	return ret;
}
 
int __txn_commit_map_get(const DB_ENV * const dbenv, const u_int64_t utxnid, DB_LSN * const commit_lsn) 
{
	const DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;

	Pthread_mutex_lock(&txmap->txmap_mutexp);
	const UTXNID_TRACK * const txn = hash_find(txmap->transactions, &utxnid);
	if (txn) {
		*commit_lsn = txn->commit_lsn;
	}
	Pthread_mutex_unlock(&txmap->txmap_mutexp);
	return txn ? 0 : DB_NOTFOUND;
}

int __txn_commit_map_add_nolock(const DB_ENV * const dbenv, const u_int64_t utxnid, const DB_LSN commit_lsn) 
{
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;
	LOGFILE_TXN_LIST * logfile_list = NULL;
	UTXNID_TRACK * txn = NULL;

	const int commit_map_debug = dbenv->attr.commit_map_debug;
	__commit_map_debug(commit_map_debug, "Trying to add utxnid %"PRIu64" commit lsn %"PRIu32":%"PRIu32" to the map\n",
		utxnid, commit_lsn.file, commit_lsn.offset);

	if ((utxnid == 0) || IS_ZERO_LSN(commit_lsn)) {
		__commit_map_debug(commit_map_debug, "Transaction is not eligible to be added to the map.\n");
		return 0;
	}

	txn = hash_find(txmap->transactions, &utxnid);

	if (txn != NULL) { 
		__commit_map_debug(commit_map_debug, "Transaction already exists in the map. Not adding it again\n");
		return 0;
	}

	if (__os_malloc(dbenv, sizeof(UTXNID_TRACK), &txn)) { return ENOMEM; }

	logfile_utxnid_list = hash_find(txmap->logfile_lists, &commit_lsn.file);

	if (!logfile_utxnid_list) {
		if (__os_malloc(dbenv, sizeof(LOGFILE_TXN_LIST), &logfile_utxnid_list)) {
			free(txn);
			return ENOMEM;
		}

		logfile_utxnid_list->file_num = commit_lsn.file;
		logfile_utxnid_list->commit_utxnids = hash_init_o(offsetof(UTXNID_TRACK, utxnid), sizeof(u_int64_t));
		hash_add(txmap->logfile_utxnid_lists, logfile_utxnid_list);

		if (commit_lsn.file < txmap->smallest_logfile || txmap->smallest_logfile == -1) {
			txmap->smallest_logfile = commit_lsn.file;
		}
	}

	if (log_compare(&txmap->modsnap_start_lsn, &commit_lsn) <= 0) {
		txmap->modsnap_start_lsn = commit_lsn;
		txmap->highest_logfile = commit_lsn.file;
	}

	txn->utxnid = utxnid;
	txn->commit_lsn = commit_lsn;
	hash_add(txmap->transactions, txn);
	hash_add(logfile_utxnid_list->commit_utxnids, txn);
	
	return 0;
}

int __txn_commit_map_add(dbenv, utxnid, commit_lsn) 
	DB_ENV *dbenv;
	u_int64_t utxnid;
	DB_LSN commit_lsn;
{
	Pthread_mutex_lock(&dbenv->txmap->txmap_mutexp);
	const int ret = __txn_commit_map_add_nolock(dbenv, utxnid, commit_lsn);
	Pthread_mutex_unlock(&dbenv->txmap->txmap_mutexp);
	return ret;
}

void __txn_commit_map_set_modsnap_start_lsn(dbenv, modsnap_start_lsn)
	DB_ENV *dbenv;
	DB_LSN modsnap_start_lsn;
{
	DB_TXN_COMMIT_MAP * const txmap = dbenv->txmap;

	Pthread_mutex_lock(&txmap->txmap_mutexp);
	txmap->modsnap_start_lsn = modsnap_start_lsn;
	Pthread_mutex_unlock(&txmap->txmap_mutexp);
}
