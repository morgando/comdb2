#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: db_rec.c,v 11.48 2003/08/27 03:54:18 ubell Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/hash.h"
#include "dbinc/db_swap.h"
#include "dbinc/txn.h"
#include "logmsg.h"

extern int __db_addrem_redo_add_undo_del(DBC *, __db_addrem_args *, PAGE *, db_recops op, DB_LSN *lsnp);
extern int __db_addrem_undo_add_redo_del(DBC *, __db_addrem_args *, PAGE *, db_recops op, DB_LSN *lsnp);
extern void __db_big_redo_add_undo_del(DB *, PAGE *, __db_big_args *, db_recops, DB_LSN *);
extern int __db_addrem_verify_fileid(DB_ENV *, DB *, DB_LSN *, DB_LSN *, int32_t);

extern void __db_relink_next_add_undo_rem_redo(PAGE *, __db_relink_args *, db_recops, DB_LSN *);
extern void __db_relink_next_add_redo_rem_undo(PAGE *, __db_relink_args *, db_recops, DB_LSN *);
extern void __db_relink_target_rem_undo(PAGE *, __db_relink_args *);
extern void __db_relink_prev_rem_undo(PAGE *, __db_relink_args *);

extern void __db_pg_free_undo(PAGE *pagep, __db_pg_freedata_args *argp, int data);
extern void __db_pg_free_meta_undo(DBMETA *meta, __db_pg_freedata_args *argp);

extern void __db_pg_alloc_target_undo(DB *file_dbp, PAGE *pagep, __db_pg_alloc_args *argp);
extern void __db_pg_alloc_meta_undo(DB *file_dbp, DBMETA *meta, __db_pg_alloc_args *argp);

extern void __db_ovref_undo(PAGE *pagep, __db_ovref_args *argp);

/*
 * PUBLIC: int __db_addrem_snap_recover
 * PUBLIC:    __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__db_addrem_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__db_addrem_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret;

	ret = 0;
	REC_INTRO(__db_addrem_read, 1);

	if (dbenv->attr.debug_addrem_dbregs) {
		// TODO: do I need to do this?
		ret =
		    __db_addrem_verify_fileid(dbenv, file_dbp, lsnp,
		    &argp->prev_lsn, argp->fileid);
		if (ret) {
			goto out;
		}
	}

	if (IS_REM_OPCODE(argp->opcode)) {
 		if ((ret = __db_addrem_redo_add_undo_del(dbc, argp, pagep, op, NULL)) != 0) {
			goto out;
		}
	} else if (argp->opcode == DB_ADD_DUP) { 
 		if ((ret = __db_addrem_undo_add_redo_del(dbc, argp, pagep, op, NULL)) != 0) {
			goto out;
		}
	} else {
		abort();
	}

out:
done:
	REC_CLOSE;
}

/*
 * PUBLIC: int __db_ovref_snap_recover
 * PUBLIC:    __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__db_ovref_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__db_ovref_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret;

	ret = 0;
	REC_INTRO(__db_ovref_read, 1);

	__db_ovref_undo(pagep, argp);

out:
done:
	REC_CLOSE;
}

/*
 * PUBLIC: int __db_big_snap_recover
 * PUBLIC:    __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__db_big_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__db_big_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret;

	ret = 0;
	REC_INTRO(__db_big_read, 1);

	if (PGNO(pagep) == argp->pgno) {
		if (argp->opcode == DB_REM_BIG) {
			__db_big_redo_add_undo_del(file_dbp, pagep, argp, op, lsnp);
		} else {
			__db_big_undo_add_redo_del(pagep, argp, op, lsnp);
		}
	} else if (PGNO(pagep) == argp->prev_pgno) {
		__db_big_prev_redo_del_undo_add(pagep, argp, op, lsnp);
	} else if (PGNO(pagep) == argp->next_pgno) {
		__db_big_next_undo(pagep, argp);
	}

out:
done:
	REC_CLOSE;
}

/*
 * PUBLIC: int __db_relink_snap_recover
 * PUBLIC:    __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__db_relink_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__db_relink_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret;

	ret = 0;
	REC_INTRO(__db_relink_read, 1);

	if (argp->opcode == DB_ADD_PAGE) {
		if (argp->pgno == PGNO(pagep)) {
			abort();
		} else if (argp->next == PGNO(pagep)) {
			__db_relink_next_add_undo_rem_redo(pagep, argp, op, NULL);
		} else if (argp->prev == PGNO(pagep)) {
			abort();
		}
	} else if(argp->opcode == DB_REM_PAGE) {
		if (argp->pgno == PGNO(pagep)) {
			__db_relink_target_rem_undo(pagep, argp);
		} else if (argp->next == PGNO(pagep)) {
			__db_relink_next_add_redo_rem_undo(pagep, argp, op, NULL);
		} else if (argp->prev == PGNO(pagep)) {
			__db_relink_prev_rem_undo(pagep, argp);
		}
	}

out:
done:
	REC_CLOSE;
}

/*
 * PUBLIC: int __db_pg_freedata_snap_recover
 * PUBLIC:    __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__db_pg_freedata_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__db_pg_freedata_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret;

	ret = 0;
	REC_INTRO(__db_pg_freedata_read, 1);

	if (argp->pgno == PGNO(pagep)) {
		__db_pg_free_undo(pagep, argp, 1);
	} else if (PGNO_BASE_MD == PGNO(pagep)) {
		__db_pg_free_meta_undo((DBMETA *) pagep, argp);
	}

out:
done:
	REC_CLOSE;
}

/*
 * PUBLIC: int __db_pg_free_snap_recover
 * PUBLIC:    __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__db_pg_free_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__db_pg_free_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret;

	ret = 0;
	REC_INTRO(__db_pg_free_read, 1);

	if (argp->pgno == PGNO(pagep)) {
		__db_pg_free_undo(pagep, (__db_pg_freedata_args *) argp, 0);
	} else if (PGNO_BASE_MD == PGNO(pagep)) {
		__db_pg_free_meta_undo((DBMETA *) pagep, (__db_pg_freedata_args *) argp);
	}

out:
done:
	REC_CLOSE;
}

/*
 * PUBLIC: int __db_pg_alloc_snap_recover
 * PUBLIC:    __P((DB_ENV *, DBT *, DB_LSN *, db_recops, PAGE *));
 */
int
__db_pg_alloc_snap_recover(dbenv, dbtp, lsnp, op, pagep)
	DB_ENV *dbenv;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	PAGE *pagep;
{
	__db_pg_alloc_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	int ret;

	ret = 0;
	REC_INTRO(__db_pg_alloc_read, 1);

	if (argp->pgno == PGNO(pagep)) {
		__db_pg_alloc_target_undo(file_dbp, pagep, (__db_pg_alloc_args *) argp);
	} else if (PGNO_BASE_MD == PGNO(pagep)) {
		__db_pg_free_meta_undo((DBMETA *) pagep, (__db_pg_freedata_args *) argp);
		if (argp->pgno > ((DBMETA *) pagep)->last_pgno) {
			((DBMETA *) pagep)->last_pgno = argp->pgno;
		}
	}

out:
done:
	REC_CLOSE;
}

