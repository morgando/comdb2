/*
 * The following functions provide a map interface specifically
 * designed to map the utxnids of uniquely-identified
 * committed transactions to their commit LSNs.
 *
 * *Note: A transaction with utxnid=0 is 
 * 		NOT uniquely identified (this is a dummy ID used for internal transactions).
 *		A transaction with any other utxnid is uniquely identified.
 *
 * Create and initialize the map by calling `__txn_commit_map_init`
 *
 * Destroy the map by calling `__txn_commit_map_destroy`
 *
 * Add a transaction to the map by calling `__txn_commit_map_add`
 *
 * Remove a transaction from the map by calling `__txn_commit_map_remove`
 *
 * Get the commit LSN of a transaction from its utxnid by calling `__txn_commit_map_get`
 *
 */

/*
 * 	Creates and initializes the transaction commit map.
 */
int __txn_commit_map_init(DB_ENV *dbenv) 

/*
 *      Destroy commit LSN map.
 */
int __txn_commit_map_destroy(DB_ENV *dbenv)

/*
 *  Removes a transaction from the commit LSN map
 *
 *  *If this function deletes the transaction with the highest commit LSN, 
 * then it is the caller's responsibility to call 
 * `__txn_commit_map_set_modsnap_start_lsn` with the next highest
 * commit LSN*
 */
int __txn_commit_map_remove(const DB_ENV * const dbenv, const u_int64_t utxnid);

/*
 *  Get the highest checkpoint lsn
 *  from the commit LSN map.
 */
void __txn_commit_map_get_highest_checkpoint_lsn(const DB_ENV * const dbenv, DB_LSN * const highest_checkpoint_lsn, const int lk_over_access);

/*
 *  Get the highest commit lsn
 *  from the commit LSN map.
 */
void __txn_commit_map_get_modsnap_start_lsn(DB_ENV * const dbenv, DB_LSN * const modsnap_start_lsn, const int lk_over_access);

/*
 *	Prints the internal state of the commit LSN map.
 */
void __txn_commit_map_print_info(const DB_ENV * const dbenv, const loglvl lvl, const int lk_over_access);

/*
 *  Remove all transactions that committed in a specific logfile 
 *  from the commit LSN map.	
 *
 *  *If this function deletes the transaction with the highest commit LSN, 
 * then it is the caller's responsibility to call 
 * `__txn_commit_map_set_modsnap_start_lsn` with the next highest
 * commit LSN*
 */
int __txn_commit_map_delete_logfile_txns(DB_ENV * const dbenv, const int logfile) 

/*
 *  Get the commit LSN of a transaction.
 */
int __txn_commit_map_get(const DB_ENV * const dbenv, const u_int64_t utxnid, DB_LSN * const commit_lsn) 

/*
 *  Store the commit LSN of a transaction.
 */
int __txn_commit_map_add_nolock(const DB_ENV * const dbenv, const u_int64_t utxnid, const DB_LSN commit_lsn) 

/*
 *  Store the commit LSN of a transaction.
 *
 * PUBLIC: int __txn_commit_map_add
 * PUBLIC:     __P((DB_ENV *, u_int64_t, DB_LSN));
 */
int __txn_commit_map_add(const DB_ENV * const dbenv, const u_int64_t utxnid, const DB_LSN commit_lsn) 

/*
 *  Set the highest commit LSN.
 *
 * PUBLIC: int __txn_commit_map_set_modsnap_start_lsn
 * PUBLIC:     __P((DB_ENV *, DB_LSN));
 */
void __txn_commit_map_set_modsnap_start_lsn(const DB_ENV * const dbenv, const DB_LSN modsnap_start_lsn)
