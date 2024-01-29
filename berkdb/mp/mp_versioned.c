
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

#define PAGE_VERSION_IS_GUARANTEED_TARGET(highest_commit_lsn_asof_checkpoint, smallest_logfile, target_lsn, pglsn) (log_compare(&highest_commit_lsn_asof_checkpoint, &pglsn) >= 0 || IS_NOT_LOGGED_LSN(pglsn) || (pglsn.file < smallest_logfile))

static int DEBUG_PAGES = 0;
static int DEBUG_PAGES1 = 0;

extern int __txn_commit_map_get(DB_ENV *, u_int64_t, DB_LSN *);

extern int __mempv_cache_init(DB_ENV *, MEMPV_CACHE *cache, int size);
extern int __mempv_cache_get(DB *dbp, MEMPV_CACHE *cache, u_int8_t file_id[DB_FILE_ID_LEN], db_pgno_t pgno, DB_LSN target_lsn, BH *bhp);
extern int __mempv_cache_put(DB *dbp, MEMPV_CACHE *cache, u_int8_t file_id[DB_FILE_ID_LEN], db_pgno_t pgno, BH *bhp, DB_LSN target_lsn);

int gbl_modsnap_cache_hits = 0;
int gbl_modsnap_cache_misses = 0;
int gbl_modsnap_total_requests = 0;

pthread_mutex_t gbl_modsnap_stats_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * __mempv_init --
 *	Initialize versioned memory pool.
 *
 * PUBLIC: int __mempv_init
 * PUBLIC:	   __P((DB_ENV *, int));
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
 *	Destroy versioned memory pool.
 *
 * PUBLIC: int __mempv_destroy
 * PUBLIC:	   __P((DB_ENV *));
 */
int __mempv_destroy(dbenv)
	DB_ENV *dbenv;
{
	return 1;
}

static int __mempv_read_log_record(DB_ENV *dbenv, void *data, int (**apply)(DB_ENV*, DBT*, DB_LSN*, db_recops, PAGE *), DB_LSN *prevPageLsn, u_int64_t *utxnid, db_pgno_t pgno) {
	int ret, utxnid_logged;
	u_int32_t rectype;

	ret = 0;

	LOGCOPY_32(&rectype, data);
	
	if ((utxnid_logged = normalize_rectype(&rectype)) != 1) {
		ret = 1;
		goto done;
	}
	if (rectype > 1000) {
		rectype -= 1000; // ufid logging is enabled. yay!
	}

	data += sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN);
	LOGCOPY_64(utxnid, data);

	switch (rectype) {
		case DB___db_addrem:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is addrem \n");
			}
			*apply = __db_addrem_snap_recover;
			break;
		case DB___db_big:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is db big \n");
			}
			*apply = __db_big_snap_recover;
			break;
		case DB___db_ovref:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is ovref \n");
			}
			*apply = __db_ovref_snap_recover;
			break;
		case DB___db_relink:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is relink \n");
			}
			*apply = __db_relink_snap_recover;
			break;
		case DB___db_pg_alloc:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is pg alloc \n");
			}
			*apply = __db_pg_alloc_snap_recover;
			break;
		case DB___bam_split:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is bam split\n");
			}
		   *apply = __bam_split_snap_recover;
		   break;
		case DB___bam_rsplit:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is bam rsplit\n");
			}
		   *apply = __bam_rsplit_snap_recover;
		   break;
		case DB___bam_repl:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is bam repl\n");
			}
		   *apply = __bam_repl_snap_recover;
		   break;
		case DB___bam_adj:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is bam adj\n");
			}
		   *apply = __bam_adj_snap_recover;
		   break;
		case DB___bam_cadjust:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is bam cadj\n");
			}
		   *apply = __bam_cadjust_snap_recover;
		   break;
		case DB___bam_cdel: 
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is bam cdel\n");
			}
		   *apply = __bam_cdel_snap_recover;
		   break;
		case DB___bam_prefix:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is bam prefix\n");
			}
		   *apply = __bam_prefix_snap_recover;
		   break;
		case DB___db_pg_freedata:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is pg freedata\n");
			}
		   *apply = __db_pg_freedata_snap_recover;
		   break;
		case DB___db_pg_free:
			if (DEBUG_PAGES1) {
				logmsg(LOGMSG_USER, "Op type of log record is pg free\n");
			}
		   *apply = __db_pg_free_snap_recover;
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
 *	Return a page in the version that it was at a past LSN.
 *
 * PUBLIC: int __mempv_fget
 * PUBLIC:	   __P((DB_MPOOLFILE *, DB *, db_pgno_t, DB_LSN, DB_LSN, void *, u_int32_t));
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
	int (*apply)(DB_ENV*, DBT*, DB_LSN*, db_recops, PAGE *);
	int add_to_cache, found, ret, cache_hit, cache_miss;
	u_int64_t utxnid;
	int64_t smallest_logfile;
	u_int32_t rectype;
	DB_LOGC *logc;
	PAGE *page, *page_image;
	DB_LSN curPageLsn, prevPageLsn, commit_lsn, highest_commit_lsn_asof_checkpoint;
	DB_ENV *dbenv;
	BH *bhp;
	void *data_t;

	ret = found = add_to_cache = cache_hit = cache_miss = 0;
	logc = NULL;
	page = page_image = NULL;
	bhp = NULL;
	data_t = NULL;
	*(void **)ret_page = NULL;
	DBT dbt = {0};
	dbt.flags = DB_DBT_REALLOC;
	dbenv = mpf->dbenv;
	highest_commit_lsn_asof_checkpoint = highest_ckpt_commit_lsn;
	Pthread_mutex_lock(&dbenv->txmap->txmap_mutexp);
	smallest_logfile = dbenv->txmap->smallest_logfile;
	Pthread_mutex_unlock(&dbenv->txmap->txmap_mutexp);

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
		//	  printf("%s: Original page has unlogged LSN or an LSN before the last checkpoint\n", __func__);
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
			bhp->is_copy = 1; // I am a copy

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
	   ret = __mempv_cache_put(dbp, &dbenv->mempv->cache, mpf->fileid, pgno, bhp, target_lsn);
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
 *	Release a page accessed with __mempv_fget.
 *
 * PUBLIC: int __mempv_fput
 * PUBLIC:	   __P((DB_MPOOLFILE *, void *, u_int32_t));
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

		if (bhp->is_copy == 1) {
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
