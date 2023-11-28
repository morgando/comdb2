
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

static int DEBUG_PAGES = 0;
static int DEBUG_PAGES1 = 0;

extern int gbl_ref_sync_iterations;
extern int gbl_ref_sync_pollms;
extern int gbl_ref_sync_wait_txnlist;

extern int __txn_commit_map_get(DB_ENV *, u_int64_t, DB_LSN *);
extern __thread DB *prefault_dbp;

extern int __mempv_cache_init(DB_ENV *, MEMPV_CACHE *cache, int size);
extern int __mempv_cache_get(DB *dbp, MEMPV_CACHE *cache, u_int8_t file_id[DB_FILE_ID_LEN], db_pgno_t pgno, DB_LSN target_lsn, BH *bhp);
extern int __mempv_cache_put(DB *dbp, MEMPV_CACHE *cache, u_int8_t file_id[DB_FILE_ID_LEN], db_pgno_t pgno, BH *bhp, DB_LSN target_lsn);

extern void hexdump(loglvl lvl, const char *key, int keylen);

extern void dopage(DB *dbp, PAGE *p);

int gbl_modsnap_cache_hits = 0;
int gbl_modsnap_cache_misses = 0;
int gbl_modsnap_total_requests = 0;

#define MAX_TXNARRAY 64
void collect_txnids(DB_ENV *dbenv, u_int32_t *txnarray, int max, int *count);
int still_running(DB_ENV *dbenv, u_int32_t *txnarray, int count);

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
				int i=0;
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
				int i=0;
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
 * PUBLIC:     __P((DB_MPOOLFILE *, DB *, db_pgno_t, DB_LSN, DB_LSN, void *, u_int32_t));
 */
int __mempv_fget(mpf, dbp, pgno, target_lsn, highest_ckpt_commit_lsn, ret_page, flags)
    DB_MPOOLFILE *mpf;
    DB *dbp;
    db_pgno_t pgno;
    DB_LSN target_lsn;
	DB_LSN highest_ckpt_commit_lsn;
    void *ret_page;
	u_int32_t flags;
{
    int (*apply)(DB_ENV*, DBT*, DB_LSN*, db_recops, void *);
    int wait_cnt, add_to_cache, found, ret, cache_hit, cache_miss, txncnt, total_txns, got_cache_page;
    u_int64_t utxnid;
    int64_t smallest_logfile;
	u_int32_t txnarray[MAX_TXNARRAY];
    u_int32_t rectype, n_cache, st_hsearch;
    DB_LOGC *logc;
	DB_MPOOL *dbmp;
	MPOOL *c_mp, *mp;
	DB_MPOOL_HASH *hp;
	MPOOLFILE *mfp;
    PAGE *page, *page_image, *cache_page_image;
    DB_LSN curPageLsn, prevPageLsn, commit_lsn, highest_commit_lsn_asof_checkpoint;
    DB_ENV *dbenv;
    BH *bhp, *src_bhp, *cache_bhp;
    void *data_t;

    DBT dbt = {0};
    dbt.flags = DB_DBT_REALLOC;
    ret = 0;
    found = 0;
	total_txns = 0;
    add_to_cache = 0;
	got_cache_page = 0;
	cache_hit = 0;
	cache_miss = 0;
    logc = NULL;
    page = NULL;
    dbenv = mpf->dbenv;
    bhp = NULL;
	cache_bhp = NULL;
	src_bhp = NULL;
    data_t = NULL;
	dbmp = dbenv->mp_handle;
	mp = dbmp->reginfo[0].primary;
	c_mp = NULL;
	mfp = mpf->mfp;
    *(void **)ret_page = NULL;
    highest_commit_lsn_asof_checkpoint = highest_ckpt_commit_lsn;
    smallest_logfile = dbenv->txmap->smallest_logfile;

	/*
    __os_malloc(dbenv, SSZA(BH, buf) + dbp->pgsize, (void *) &cache_bhp);
    cache_page_image = (PAGE *) (((u_int8_t *) cache_bhp) + SSZA(BH, buf) );
	*/

    // Get current version of the page

    if ((ret = __memp_fget(mpf, &pgno, DB_MPOOL_SNAPGET | flags, &page)) != 0) {
        if (DEBUG_PAGES) {
            printf("%s: Failed to get initial page version\n", __func__);
        }
        ret = 1;
        goto done;
    }

    curPageLsn = LSN(page);

    if (PAGE_VERSION_IS_GUARANTEED_TARGET(highest_commit_lsn_asof_checkpoint, smallest_logfile, target_lsn, curPageLsn)) {
        if (DEBUG_PAGES) {
        //    printf("%s: Original page has unlogged LSN or an LSN before the last checkpoint\n", __func__);
			int i=0;
        }
        found = 1;
		page_image = page;
		goto search;
    } else {
		__os_malloc(dbenv, SSZA(BH, buf) + dbp->pgsize, (void *) &bhp);
		page_image = (PAGE *) (((u_int8_t *) bhp) + SSZA(BH, buf) );

		if (!page_image) {
			if (DEBUG_PAGES) {
				printf("%s: Failed to allocate page image\n", __func__);
			}
			ret = ENOMEM;
			goto done;
		}

		if (!__mempv_cache_get(dbp, &dbenv->mempv->cache, mpf->fileid, pgno, target_lsn, bhp)) {
			if (DEBUG_PAGES) {
				printf("%s: Found in cache\n", __func__);
			}
			cache_hit = 1;
			found = 1;

			if ((ret = __memp_fput(mpf, page, DB_MPOOL_SNAPPUT)) != 0) {
				printf("%s: Failed to return initial page version\n", __func__);
				ret = 1;
				goto done;
			}
		} else {
			cache_miss = 1;

			memcpy(bhp, ((char*)page) - offsetof(BH, buf), offsetof(BH, buf) + dbp->pgsize);
			bhp->ref_type = 3; // I am a copy

			if ((ret = __memp_fput(mpf, page, DB_MPOOL_SNAPPUT)) != 0) {
				printf("%s: Failed to return initial page version\n", __func__);
				ret = 1;
				goto done;
			}

			if ((ret = __log_cursor(dbenv, &logc)) != 0) {
				if (DEBUG_PAGES) {
					printf("%s: Failed to create log cursor\n", __func__);
				}
				ret = 1;
				goto done;
			}
		}

	}

search:
    while (!found) 
    {
        if (DEBUG_PAGES)
            printf("%s: Rolling back page %u with initial LSN %d:%d to prior LSN %d:%d. Highest asof checkpoint %d:%d\n", __func__, PGNO(page_image), LSN(page_image).file, LSN(page_image).offset, target_lsn.file, target_lsn.offset, highest_commit_lsn_asof_checkpoint.file,highest_commit_lsn_asof_checkpoint.offset);

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
        if (!__txn_commit_map_get(dbenv, utxnid, &commit_lsn) && (log_compare(&commit_lsn, &target_lsn) <= 0)) {
            if (DEBUG_PAGES) {
                printf("%s: %u Found the right page version\n", __func__, PGNO(page_image));
            }
            add_to_cache = 1;
            found = 1;
            ret = 0;
            break;
        }


		if (*apply == __bam_cdel_recover) {
			__bam_cdel_args *argp;
			__bam_cdel_read(dbenv, dbt.data, &argp);
			// printf("I AM DOING CDEL RECOVERY\n");
			int indx = argp->indx + (TYPE(page_image) == P_LBTREE ? O_INDX : 0);
			B_DCLR(GET_BKEYDATA(dbp, page_image, indx));

	//		(void)__bam_ca_delete(file_dbp, argp->pgno, argp->indx, 0);

			LSN(page_image) = argp->lsn;
		} else if((ret = apply(dbenv, &dbt, &curPageLsn, DB_TXN_ABORT, page_image)) != 0) {
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
	/*if (got_cache_page) {
		if (memcmp(page_image, cache_page_image, dbp->pgsize) != 0) {
			printf("pages don't match pgno cache %u lsn cache %u:%u pgno reconstructed %u lsn %u:%u\n", PGNO(cache_page_image), LSN(cache_page_image).file, LSN(cache_page_image).offset, PGNO(page_image), LSN(page_image).file, LSN(page_image).offset);
			dopage(dbp, page_image);
			dopage(dbp, cache_page_image);
			hexdump(LOGMSG_ERROR, (const char *) page_image, dbp->pgsize);
			hexdump(LOGMSG_ERROR, (const char *) cache_page_image, dbp->pgsize);
			// sleep(1);
			// abort();
		}
	}*/
    if (/*!got_cache_page && */add_to_cache == 1) {
	   ret = __mempv_cache_put(dbp, &dbenv->mempv->cache, mpf->fileid, pgno, bhp, target_lsn);
	}
	// __os_free(dbenv, cache_bhp);
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
	if (gbl_modsnap_total_requests == 1000000000) {
		gbl_modsnap_total_requests = 0;
		gbl_modsnap_cache_hits = 0;
		gbl_modsnap_cache_misses = 0;
	}
	pthread_mutex_unlock(&gbl_modsnap_stats_mutex);
    return ret;
}

/*
 * __mempv_fput --
 *  Release a page accessed with __mempv_fget.
 *
 * PUBLIC: int __mempv_fput
 * PUBLIC:     __P((DB_MPOOLFILE *, void *, u_int32_t));
 */
int __mempv_fput(mpf, page, flags)
    DB_MPOOLFILE *mpf;
    void *page;
	u_int32_t flags;
{
    BH *bhp;
    DB_ENV *dbenv;
	int rc;

	if (flags != 0) {
		printf("flags %d\n", flags);
		abort();
	}

    dbenv = mpf->dbenv;
	rc = 0;

    if (page != NULL) {
		bhp = (BH *)((u_int8_t *)page - SSZA(BH, buf));

		if (bhp->ref_type == 3) {
			// I am a copy
			__os_free(dbenv, bhp);
		} else if (__memp_fput(mpf, page, DB_MPOOL_SNAPPUT) != 0) {
			printf("%s: Failed to return initial page version\n", __func__);
			rc = 1;
		}

        page = NULL;
    }
    return rc;
}
