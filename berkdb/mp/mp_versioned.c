#include "db_int.h"
#include "dbinc/btree.h"
#include "dbinc/mp.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include "dbinc/db_swap.h"
#include "dbinc/lock.h"
#include "btree/bt_cache.h"

// TODO: Remove prevpagelsn

static int DEBUG_PAGES = 0;
static int DEBUG_PAGES1 = 0;

extern int __txn_commit_map_get(DB_ENV *, u_int64_t, DB_LSN *);
extern __thread DB *prefault_dbp;

/*
 * __mempv_init --
 *  Initialize versioned memory pool.
 *
 * PUBLIC: int __mempv_init
 * PUBLIC:     __P((DB_ENV *));
 */
int __mempv_init(dbenv)
    DB_ENV *dbenv;
{
    return 0;
}

/*
 * __mempv_destroy --
 *  Destroy versioned memory pool.
 *
 * PUBLIC: int __mempv_destroy
 * PUBLIC:     __P((DB_ENV *));
 */
int __mempv_destroy(dbenv)
    DB_ENV *dbenv;
{
    return 1;
}

static int __mempv_read_log_record(DB_ENV *dbenv, void *data, int (**apply)(DB_ENV*, DBT*, DB_LSN*, db_recops, void *), DB_LSN *prevPageLsn, u_int64_t *utxnid, db_pgno_t pgno) {
    int ret, utxnid_logged;
    u_int32_t rectype;

    ret = 0;

    LOGCOPY_32(&rectype, data);
    
    if ((utxnid_logged = normalize_rectype(&rectype)) != 1) {
        ret = 1;
        goto done;
    }

	data += sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN);
	LOGCOPY_64(utxnid, data);

    switch (rectype) {
        case DB___db_addrem:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is addrem \n");
            }
            *apply = __db_addrem_recover;
            break;
        case DB___db_big:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is db big \n");
            }
            *apply = __db_big_recover;
            break;
        case DB___db_ovref:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is ovref \n");
            }
            *apply = __db_ovref_recover;
            break;
        case DB___db_relink:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is relink \n");
            }
            *apply = __db_relink_recover; // TODO single page recover.
            break;
        case DB___db_pg_alloc:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is pg alloc \n");
            }
            *apply = __db_pg_alloc_recover;
            break;
        case DB___bam_split:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is bam split\n");
            }
           *apply = __bam_split_recover;
           break;
        case DB___bam_rsplit:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is bam rsplit\n");
            }
           *apply = __bam_rsplit_recover;
           break;
        case DB___bam_repl:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is bam repl\n");
            }
           *apply = __bam_repl_recover;
           break;
        case DB___bam_adj:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is bam adj\n");
            }
           *apply = __bam_adj_recover;
           break;
        case DB___bam_cadjust:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is bam cadj\n");
            }
           *apply = __bam_cadjust_recover;
           break;
        case DB___bam_cdel: 
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is bam cdel\n");
            }
           *apply = __bam_cdel_recover;
           break;
        case DB___bam_prefix:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is bam prefix\n");
            }
           *apply = __bam_prefix_recover;
           break;
        case DB___db_pg_freedata:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is pg freedata\n");
            }
           *apply = __db_pg_freedata_recover;
           break;
        case DB___db_pg_free:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is pg free\n");
            }
           *apply = __db_pg_free_recover;
           break;
            
        /*case DB___bam_pgcompact:
           // TODO
           *apply = __bam_pgcompact_recover;
           break;*/
        default:
            if (DEBUG_PAGES1) {
                logmsg(LOGMSG_USER, "Op type of log record is unrecognized %d\n", rectype);
            }
            ret = 1;
            goto done;
            break;
    }
done:       
    return ret;
}

static int __mempv_page_version_is_guaranteed_target(dbenv, target_lsn, page_image)
    DB_ENV *dbenv;
    DB_LSN target_lsn;
    PAGE *page_image;
{
	DB_LSN pglsn;

	pglsn = LSN(page_image);

    return (IS_NOT_LOGGED_LSN(pglsn) || (pglsn.file < dbenv->txmap->smallest_logfile) || (log_compare(&target_lsn, &dbenv->txmap->highest_commit_lsn_asof_checkpoint) >= 0 && log_compare(&dbenv->txmap->highest_commit_lsn_asof_checkpoint, &pglsn) >= 0));
}


/*
 * __mempv_fget --
 *  Return a page in the version that it was at a past LSN.
 *
 * PUBLIC: int __mempv_fget
 * PUBLIC:     __P((DB_MPOOLFILE *, DB *, db_pgno_t, DB_LSN, void *));
 */
int __mempv_fget(mpf, dbp, pgno, target_lsn, ret_page)
    DB_MPOOLFILE *mpf;
    DB *dbp;
    db_pgno_t pgno;
    DB_LSN target_lsn;
    void *ret_page;
{
    int (*apply)(DB_ENV*, DBT*, DB_LSN*, db_recops, void *);
    int have_lock, have_page, have_page_image, found, ret;
    u_int64_t utxnid;
    u_int32_t rectype;
    DB_LOGC *logc;
    PAGE *page, *page_image;
    DB_LSN curPageLsn, prevPageLsn, commit_lsn;
    DB_ENV *dbenv;
    BH *bhp;
    void *data_t;

    DBT dbt = {0};
    dbt.flags = DB_DBT_REALLOC;
    ret = 0;
    found = 0;
    logc = NULL;
    data_t = NULL;
    *(void **)ret_page = NULL;
    dbenv = mpf->dbenv;
    __os_malloc(dbenv, offsetof(BH, buf) + dbp->pgsize, (void *) &bhp);
    page_image = (PAGE *) (((char *) bhp) + offsetof(BH, buf));

    if (!page_image) {
        if (DEBUG_PAGES) {
            printf("%s: Failed to allocate page image\n", __func__);
        }
        ret = ENOMEM;
        goto done;
    }

    // Get current version of the page
    if ((ret = __memp_fget(mpf, &pgno, 0, &page)) != 0) {
        if (DEBUG_PAGES) {
            printf("%s: Failed to get initial page version\n", __func__);
        }
        ret = 1;
        goto done;
    }

    memcpy(bhp, ((char*)page) - offsetof(BH, buf), offsetof(BH, buf) + dbp->pgsize);

    if ((ret = __memp_fput(mpf, page, 0)) != 0) {
        printf("%s: Failed to return initial page version\n", __func__);
        ret = 1;
        goto done;
    }

    if (ret) {
        goto done;
    }

    if (__mempv_page_version_is_guaranteed_target(dbenv, target_lsn, page_image)) {
        if (DEBUG_PAGES) {
            printf("%s: Original page has unlogged LSN or an LSN before the last checkpoint\n", __func__);
        }
        found = 1;
    } else if ((ret = __log_cursor(dbenv, &logc)) != 0) {
        if (DEBUG_PAGES) {
            printf("%s: Failed to create log cursor\n", __func__);
        }
        ret = 1;
        goto done;
    }

search:
    while (!found) 
    {
        curPageLsn = LSN(page_image);

        if (DEBUG_PAGES)
            printf("%s: Rolling back page %u with initial LSN %d:%d to prior LSN %d:%d\n", __func__, PGNO(page_image), LSN(page_image).file, LSN(page_image).offset, target_lsn.file, target_lsn.offset);

        if (__mempv_page_version_is_guaranteed_target(dbenv, target_lsn, page_image)) {
            found = 1;
            break;
        }

        if (IS_ZERO_LSN(curPageLsn)) {
            if (DEBUG_PAGES) {
                printf("%s: Got to page with zero / unlogged LSN\n", __func__);
            }
            ret = 1;
            goto done;
        }
        
        // Move log cursor
        ret = __log_c_get(logc, &curPageLsn, &dbt, DB_SET);
        if (ret || (dbt.size < sizeof(int))) {
            if (DEBUG_PAGES) {
                printf("%s: Failed to get log cursor\n", __func__);
            }
            ret = 1;
            goto done;
        }

        if ((ret = __mempv_read_log_record(dbenv, data_t != NULL ? data_t : dbt.data, &apply, &prevPageLsn, &utxnid, PGNO(page_image))) != 0) {
            if (DEBUG_PAGES) {
                printf("%s: Failed to read log record\n", __func__);
            }
            ret = 1;
            goto done;
        }

         // If the transaction that wrote this page is still in-progress or it committed before our target LSN, return this page.
        if (__txn_commit_map_get(dbenv, utxnid, &commit_lsn) || (log_compare(&commit_lsn, &target_lsn) <= 0)) {
            if (DEBUG_PAGES) {
                printf("%s: Found the right page version\n", __func__);
            }
            found = 1;
            ret = 0;
            break;
        }

        if((ret = apply(dbenv, &dbt, &curPageLsn, DB_TXN_ABORT, page_image)) != 0) {
            if (DEBUG_PAGES) {
                printf("%s: Failed to undo log record\n", __func__);
            }
            ret = 1;
            goto done;
        }
    }

    *(void **)ret_page = (void *) page_image;
done:
    if (dbt.data) {
        __os_free(dbenv, dbt.data);
		dbt.data = NULL;
	}
	if (logc) {
		__log_c_close(logc);
	}
    return ret;
}

/*
 * __mempv_fput --
 *  Release a page accessed with __mempv_fget.
 *
 * PUBLIC: int __mempv_fput
 * PUBLIC:     __P((DB_MPOOLFILE *, void *));
 */
int __mempv_fput(mpf, page)
    DB_MPOOLFILE *mpf;
    void *page;
{
    BH *bhp;
    DB_ENV *dbenv;

    dbenv = mpf->dbenv;

    if (page != NULL) {
        bhp = (BH *) (((char*)page)-offsetof(BH, buf));
        __os_free(dbenv, bhp);
        page = NULL;
    }
    return 0;
}
