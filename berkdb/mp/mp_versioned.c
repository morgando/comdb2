
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <alloca.h>
#include <limits.h>
#include <sys/types.h>
#include <limits.h>
#include <string.h>
#include <pthread.h>
#include <poll.h>

#include "db_config.h"
#include "db_int.h"
#include "dbinc/btree.h"
#include "dbinc/mp.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include "dbinc/db_swap.h"
#include "dbinc/lock.h"
#include "dbinc/mutex.h"
#include "btree/bt_cache.h"
#include "dbinc/db_shash.h"
#include "dbinc/hmac.h"
#include "dbinc_auto/hmac_ext.h"

#include "thdpool.h"
#include "ctrace.h"
#include "logmsg.h"
#include "comdb2_atomic.h"
#include "thrman.h"
#include "thread_util.h"
#include "thread_stats.h"
#include <pool.h>
#include "locks_wrap.h"

// TODO: Remove prevpagelsn

#define PAGE_VERSION_IS_GUARANTEED_TARGET(highest_commit_lsn_asof_checkpoint, smallest_logfile, target_lsn, pglsn) ((log_compare(&target_lsn, &highest_commit_lsn_asof_checkpoint) >= 0 && log_compare(&highest_commit_lsn_asof_checkpoint, &pglsn) >= 0) || IS_NOT_LOGGED_LSN(pglsn) || (pglsn.file < smallest_logfile))

static int DEBUG_PAGES = 1;
static int DEBUG_PAGES1 = 0;

extern int gbl_ref_sync_iterations;
extern int gbl_ref_sync_pollms;

extern int __txn_commit_map_get(DB_ENV *, u_int64_t, DB_LSN *);
extern __thread DB *prefault_dbp;

extern int __mempv_cache_init(DB_ENV *, MEMPV_CACHE *cache, int size);
extern int __mempv_cache_get(DB *dbp, MEMPV_CACHE *cache, u_int8_t file_id[DB_FILE_ID_LEN], db_pgno_t pgno, DB_LSN target_lsn, BH *bhp);
extern int __mempv_cache_put(DB *dbp, MEMPV_CACHE *cache, u_int8_t file_id[DB_FILE_ID_LEN], db_pgno_t pgno, BH *bhp, DB_LSN target_lsn);

int gbl_modsnap_cache_hits = 0;
int gbl_modsnap_cache_misses = 0;
int gbl_modsnap_total_requests = 0;

pthread_mutex_t gbl_modsnap_stats_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * __mempv_init --
 *  Initialize versioned memory pool.
 *
 * PUBLIC: int __mempv_init
 * PUBLIC:     __P((DB_ENV *, int));
 */
int __mempv_init(dbenv, size)
    DB_ENV *dbenv;
	int size;
{
    DB_MEMPV *mempv;
    int ret;

    ret = __os_malloc(dbenv, sizeof(DB_MEMPV), &mempv);
    if (ret) {
        goto done;
    }

    if ((ret = __mempv_cache_init(dbenv, &(mempv->cache), size)), ret != 0) {
        goto done;
    }

    dbenv->mempv = mempv;

done:
    return ret;
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
    int wait_cnt, add_to_cache, found, ret, cache_hit, cache_miss;
    u_int64_t utxnid;
    int64_t smallest_logfile;
    u_int32_t rectype, n_cache, st_hsearch;
    DB_LOGC *logc;
	DB_MPOOL *dbmp;
	MPOOL *c_mp, *mp;
	DB_MPOOL_HASH *hp;
	MPOOLFILE *mfp;
    PAGE *page, *page_image;
    DB_LSN curPageLsn, prevPageLsn, commit_lsn, highest_commit_lsn_asof_checkpoint;
    DB_ENV *dbenv;
    BH *bhp, *src_bhp;
    void *data_t;

    DBT dbt = {0};
    dbt.flags = DB_DBT_REALLOC;
    ret = 0;
    found = 0;
    add_to_cache = 0;
	cache_hit = 0;
	cache_miss = 0;
    logc = NULL;
    page = NULL;
    dbenv = mpf->dbenv;
    bhp = NULL;
	src_bhp = NULL;
    data_t = NULL;
	dbmp = dbenv->mp_handle;
	mp = dbmp->reginfo[0].primary;
	c_mp = NULL;
	mfp = mpf->mfp;
    *(void **)ret_page = NULL;
    highest_commit_lsn_asof_checkpoint = dbenv->txmap->highest_commit_lsn_asof_checkpoint;
    smallest_logfile = dbenv->txmap->smallest_logfile;
    __os_malloc(dbenv, SSZA(BH, buf) + dbp->pgsize, (void *) &bhp);
    page_image = (PAGE *) (((u_int8_t *) bhp) + SSZA(BH, buf) );

	printf("SSZA %d offsetof %ld\n", SSZA(BH, buf), offsetof(BH, buf));
    if (!page_image) {
        if (DEBUG_PAGES) {
            printf("%s: Failed to allocate page image\n", __func__);
        }
        ret = ENOMEM;
        goto done;
    }

	int rs_iters = gbl_ref_sync_iterations;
	int rs_pollms = gbl_ref_sync_pollms;

	rs_iters = (rs_iters > 0 ? rs_iters : 4);
	rs_pollms = (rs_pollms > 0 ? rs_pollms : 250);

retry:
    if ((ret = __memp_fget(mpf, &pgno, 0, &page)) != 0) {
        if (DEBUG_PAGES) {
            printf("%s: Failed to get initial page version\n", __func__);
        }
        ret = 1;
        goto done;
    }

	src_bhp = (BH *)((u_int8_t *)page - SSZA(BH, buf));

//	printf("%s Got initial page version with %d refs\n", __func__, src_bhp->ref);

	n_cache = NCACHE(mp, mfp, pgno);
	c_mp = dbmp->reginfo[n_cache].primary;
	hp = R_ADDR(&dbmp->reginfo[n_cache], c_mp->htab);
	hp = &hp[NBUCKET(c_mp, mfp, pgno)];

	st_hsearch = 0;
	MUTEX_LOCK(dbenv, &hp->hash_mutex);
	// printf("%s Locked hash lock\n", __func__);

	/*
	 * The buffer is either pinned or dirty.
	 *
	 * Set the sync wait-for count, used to count down outstanding
	 * references to this buffer as they are returned to the cache.
	 */

	/* Pin the buffer into memory and lock it. */
	if (F_ISSET(src_bhp, BH_LOCKED)) {
		MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
		if ((ret = __memp_fput(mpf, page, 0)) != 0) {
			printf("%s: Failed to return initial page version\n", __func__);
			ret = 1;
			goto done;
		}
		poll(NULL, 0, rs_pollms);
	//	printf("%s Buffer already locked. Retrying page access\n", __func__);
		goto retry;
	}
	MUTEX_LOCK(dbenv, &src_bhp->mutex);

	src_bhp->ref_sync = src_bhp->ref-1; // Must subtract 1 to account for our ref.
	F_SET(src_bhp, BH_LOCKED); 
	if (src_bhp->ref == 0) {
		abort();
	}

	/*
	 * Unlock the hash bucket and wait for the wait-for count to
	 * go to 0.   No new thread can acquire the buffer because we
	 * have it locked.
	 *
	 * If a thread attempts to re-pin a page, the wait-for count
	 * will never go to 0 (the thread spins on our buffer lock,
	 * while we spin on the thread's ref count).  Give up if we
	 * don't get the buffer in 3 seconds, we can try again later.
	 *
	 * If, when the wait-for count goes to 0, the buffer is found
	 * to be dirty, write it.
	 */
	MUTEX_UNLOCK(dbenv, &hp->hash_mutex);

	//printf("%s: Waiting for %d other refs to go away\n", __func__, src_bhp->ref_sync);
	for (wait_cnt = 0; src_bhp->ref_sync != 0 && wait_cnt < rs_iters;
			++wait_cnt) {
		poll(NULL, 0, rs_pollms);
	}

	MUTEX_LOCK(dbenv, &hp->hash_mutex);

	if (src_bhp->ref_sync != 0) {
		abort(); // TODO: Do txnwait stuff that sync code does.
	}
	
	if (src_bhp) {
		memcpy(bhp, src_bhp, SSZA(BH, buf) + dbp->pgsize);
	} else {
		abort();
	}

	if (F_ISSET(src_bhp, BH_LOCKED)) {
		F_CLR(src_bhp, BH_LOCKED);
		MUTEX_UNLOCK(dbenv, &src_bhp->mutex);
	}

	MUTEX_UNLOCK(dbenv, &hp->hash_mutex);

	if ((ret = __memp_fput(mpf, page, 0)) != 0) {
        printf("%s: Failed to return initial page version\n", __func__);
        ret = 1;
        goto done;
	}

/*
    memcpy(bhp, ((char*)page) - offsetof(BH, buf), offsetof(BH, buf) + dbp->pgsize);


	n_cache = NCACHE(mp, mfp, pgno);
	c_mp = dbmp->reginfo[n_cache].primary;
	hp = R_ADDR(&dbmp->reginfo[n_cache], c_mp->htab);
	hp = &hp[NBUCKET(c_mp, mfp, pgno)];

	st_hsearch = 0;
	MUTEX_LOCK(dbenv, &hp->hash_mutex);
	for (src_bhp = SH_TAILQ_FIRST(&hp->hash_bucket, __bh);
	    src_bhp != NULL; src_bhp = SH_TAILQ_NEXT(src_bhp, hq, __bh)) {
		++st_hsearch;
		if (src_bhp->pgno != pgno || src_bhp->mpf != mfp)
			continue;

		 * The buffer is either pinned or dirty.
		 *
		 * Set the sync wait-for count, used to count down outstanding
		 * references to this buffer as they are returned to the cache.
		src_bhp->ref_sync = src_bhp->ref;

		Pin the buffer into memory and lock it.
		++src_bhp->ref;
		F_SET(src_bhp, BH_LOCKED);
		MUTEX_LOCK(dbenv, &src_bhp->mutex);

		 * Unlock the hash bucket and wait for the wait-for count to
		 * go to 0.   No new thread can acquire the buffer because we
		 * have it locked.
		 *
		 * If a thread attempts to re-pin a page, the wait-for count
		 * will never go to 0 (the thread spins on our buffer lock,
		 * while we spin on the thread's ref count).  Give up if we
		 * don't get the buffer in 3 seconds, we can try again later.
		 *
		 * If, when the wait-for count goes to 0, the buffer is found
		 * to be dirty, write it.
		MUTEX_UNLOCK(dbenv, &hp->hash_mutex);

		int rs_iters = gbl_ref_sync_iterations;
		int rs_pollms = gbl_ref_sync_pollms;

		rs_iters = (rs_iters > 0 ? rs_iters : 4);
		rs_pollms = (rs_pollms > 0 ? rs_pollms : 250);

		for (wait_cnt = 0; src_bhp->ref_sync != 0 && wait_cnt < rs_iters;
				++wait_cnt) {
			poll(NULL, 0, rs_pollms);
 		}

		MUTEX_LOCK(dbenv, &hp->hash_mutex);
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
	*/


    /*if ((ret = __memp_fput(mpf, page, 0)) != 0) {
        printf("%s: Failed to return initial page version\n", __func__);
        ret = 1;
        goto done;
    }*/

    if (ret) {
        goto done;
    }

    curPageLsn = LSN(page_image);

    if (PAGE_VERSION_IS_GUARANTEED_TARGET(highest_commit_lsn_asof_checkpoint, smallest_logfile, target_lsn, curPageLsn)) {
        if (DEBUG_PAGES) {
            printf("%s: Original page has unlogged LSN or an LSN before the last checkpoint\n", __func__);
        }
        found = 1;
    } else if (0/*!__mempv_cache_get(dbp, &dbenv->mempv->cache, mpf->fileid, pgno, target_lsn, bhp)*/) {
		printf("Cache hit\n");
		cache_hit = 1;
        found = 1;
    } else {
		cache_miss = 1;

		if ((ret = __log_cursor(dbenv, &logc)) != 0) {
			if (DEBUG_PAGES) {
				printf("%s: Failed to create log cursor\n", __func__);
			}
			ret = 1;
			goto done;
		}
    }

search:
    while (!found) 
    {
        if (DEBUG_PAGES)
            printf("%s: Rolling back page %u with initial LSN %d:%d to prior LSN %d:%d\n", __func__, PGNO(page_image), LSN(page_image).file, LSN(page_image).offset, target_lsn.file, target_lsn.offset);

        if (PAGE_VERSION_IS_GUARANTEED_TARGET(highest_commit_lsn_asof_checkpoint, smallest_logfile, target_lsn, curPageLsn)) {
            add_to_cache = 1;
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
            add_to_cache = 1;
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

        curPageLsn = LSN(page_image);
    }

    *(void **)ret_page = (void *) page_image;
done:
    if (add_to_cache == 1) {
       // ret = __mempv_cache_put(dbp, &dbenv->mempv->cache, mpf->fileid, pgno, bhp, target_lsn);
	   int i=0;
	}
    if (logc) {
		__log_c_close(logc);
	}
    if (dbt.data) {
        __os_free(dbenv, dbt.data);
		dbt.data = NULL;
	}
	pthread_mutex_lock(&gbl_modsnap_stats_mutex);
	gbl_modsnap_total_requests++;
	if (cache_hit) {
		gbl_modsnap_cache_hits++;
	} else if (cache_miss) {
		gbl_modsnap_cache_misses++;
	}
	pthread_mutex_unlock(&gbl_modsnap_stats_mutex);
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
		bhp = (BH *)((u_int8_t *)page - SSZA(BH, buf));
        __os_free(dbenv, bhp);
        page = NULL;
    }
    return 0;
}
