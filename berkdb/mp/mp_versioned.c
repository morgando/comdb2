#include "db_int.h"
#include "dbinc/btree.h"
#include "dbinc/mp.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include "dbinc/db_swap.h"
#include "dbinc/lock.h"
#include "btree/bt_cache.h"

// TODO: Remove prevpagelsn

static int DEBUG_PAGES = 1;

extern int __txn_commit_map_get(DB_ENV *, u_int64_t, DB_LSN *);
extern __thread DB *prefault_dbp;

void *gbl_mempv_base = NULL;

/*
 * __mempv_init --
 * 	Initialize versioned memory pool.
 *
 * PUBLIC: int __mempv_init
 * PUBLIC:     __P((DB_ENV *, u_int64_t size));
 */
int __mempv_init(dbenv, size)
	DB_ENV *dbenv;
	u_int64_t size;
{
	int ret;
	DB_MEMPV *mpv;

	ret = 0;

	__os_calloc(dbenv, 1, sizeof(DB_MEMPV), &mpv);
	mpv->pages = hash_init_o(offsetof(MEMPV_PAGE_CACHE, key), sizeof(MEMPV_KEY));
	if (mpv->pages == NULL) {
		ret = ENOMEM;
		goto done;
	}

	gbl_mempv_base = malloc(size);
	if (gbl_mempv_base == NULL) {
		ret = ENOMEM;
		goto done;
	}

	mpv->msp = create_mspace_with_base(gbl_mempv_base, size, 1);
	mpv->size = size;
	dbenv->mempv = mpv;
	Pthread_mutex_init(&mpv->mempv_mutexp, NULL);
	listc_init(&mpv->lru, offsetof(MEMPV_PAGE_CACHE, lrulnk));
	
done:
	return ret;
}

/*
 * __mempv_destroy --
 * 	Destroy versioned memory pool.
 *
 * PUBLIC: int __mempv_destroy
 * PUBLIC:     __P((DB_ENV *));
 */
int __mempv_destroy(dbenv)
	DB_ENV *dbenv;
{
	return 1;
}

static int __mempv_create_page_cache(DB *dbp, db_pgno_t pgno, MEMPV_PAGE_CACHE **page_cache) {
	MEMPV_PAGE_CACHE *pc;
	int ret;

	ret = 0;

	ret = __os_malloc(dbp->dbenv, sizeof(MEMPV_PAGE_CACHE), &pc);
	if (ret)
		goto done;

	pc->key.pgno = pgno;
	memcpy(pc->key.ufid, dbp->mpf->fileid, DB_FILE_ID_LEN);
	listc_init(&pc->pages, offsetof(MEMPV_PAGE_HEADER, commit_order));
	pc->new_version = 0;
	*page_cache = pc;
	hash_add(dbp->dbenv->mempv->pages, pc);

done:
	return ret;
}

static int __mempv_read_log_record(DB_ENV *dbenv, void *data, int (**apply)(DB_ENV*, DBT*, DB_LSN*, db_recops, void *), DB_LSN *prevPageLsn, u_int64_t *utxnid, db_pgno_t pgno) {
	int ret, utxnid_logged;
	u_int32_t rectype;
	__db_addrem_args *addrem_args;
	__db_ovref_args *ovref_args;
	__db_relink_args *relink_args;
	__db_big_args *big_args;
	__db_pg_alloc_args *pg_alloc_args;
	__db_pg_free_args *pg_free_args;
	__db_pg_freedata_args *pg_freedata_args;
	__bam_split_args *split_args;
	__bam_rsplit_args *rsplit_args;
	__bam_repl_args *repl_args;
	__bam_adj_args *adj_args;
	__bam_cadjust_args *cadjust_args;
	__bam_cdel_args *cdel_args;
	__bam_prefix_args *prefix_args;
	//__bam_pgcompact_args *pgcompact_args;

	ret = 0;

	LOGCOPY_32(&rectype, data);
	
	if ((utxnid_logged = normalize_rectype(&rectype)) != 1) {
		ret = 1;
		goto done;
	}
	switch (rectype) {
		case DB___db_addrem:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is addrem \n");
			}
			if ((ret = __db_addrem_read(dbenv, data, &addrem_args)) != 0) {
				goto done;	
			}
			*prevPageLsn = addrem_args->pagelsn;
			*apply = __db_addrem_recover;
			*utxnid = addrem_args->txnid->utxnid;
			break;
		case DB___db_big:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is db big \n");
			}
			if ((ret = __db_big_read(dbenv, data, &big_args)) != 0) {
				goto done;
			}
			*prevPageLsn = big_args->pagelsn;
			*apply = __db_big_recover;
			*utxnid = big_args->txnid->utxnid;
			break;
		case DB___db_ovref:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is ovref \n");
			}
			if ((ret = __db_ovref_read(dbenv, data, &ovref_args)) != 0) {
				goto done;
			}
			*prevPageLsn = ovref_args->lsn;
			*apply = __db_ovref_recover;
			*utxnid = ovref_args->txnid->utxnid;
			break;
		case DB___db_relink:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is relink \n");
			}
			if ((ret = __db_relink_read(dbenv, data, &relink_args)) != 0) {
				goto done;
			}
			if (pgno == relink_args->pgno) {
				*prevPageLsn = relink_args->lsn;
			} else if (pgno == relink_args->prev) {
				*prevPageLsn = relink_args->lsn_prev;
			} else if (pgno == relink_args->next) {
				*prevPageLsn = relink_args->lsn_next;
			} else {
				ret = 1;
				goto done;
			}
			*apply = __db_relink_recover; // TODO single page recover.
			*utxnid = relink_args->txnid->utxnid;
			break;
		case DB___db_pg_alloc:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is pg alloc \n");
			}
			if ((ret = __db_pg_alloc_read(dbenv, data, &pg_alloc_args)) != 0) {
				goto done;
			}
			*prevPageLsn = pg_alloc_args->page_lsn;
			*apply = __db_pg_alloc_recover;
			*utxnid = pg_alloc_args->txnid->utxnid;
			break;
		case DB___bam_split:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is bam split\n");
			}
		   if ((ret = __bam_split_read(dbenv, data, &split_args)) != 0) {
			   goto done;
		   }

		   if (pgno == split_args->left) {
			   *prevPageLsn = split_args->llsn;
			   if (DEBUG_PAGES) {
				   logmsg(LOGMSG_USER, "Split: Page %d is left page\n", pgno);
			   }
		   } else if (pgno == split_args->npgno) {
			   *prevPageLsn = split_args->nlsn;
			   if (DEBUG_PAGES) {
				   logmsg(LOGMSG_USER, "Split: Page %d is n page\n", pgno);
			   }
		   } else if (pgno == split_args->right) {
			   *prevPageLsn = split_args->rlsn;
			   if (DEBUG_PAGES) {
				   logmsg(LOGMSG_USER, "Split: Page %d is right page\n", pgno);
			   }
			   // this should not happen. Assert?
		   } else if (pgno == split_args->root_pgno) {
			   *prevPageLsn = LSN(split_args->pg.data); 
			   if (DEBUG_PAGES) {
				   logmsg(LOGMSG_USER, "Split: Page %d is root page\n", pgno);
			   }
		   } else {
			   // this should not happen.
			   if (DEBUG_PAGES) {
				   logmsg(LOGMSG_USER, "Split: Page %d is not left page %d not n page %d not right page %d not root page %d\n", pgno, split_args->left, split_args->npgno, split_args->right, split_args->root_pgno);
			   }
			   ret = 1;
			   goto done;
		   }

		   *apply = __bam_split_recover;
		   *utxnid = split_args->txnid->utxnid;
		   break;
		case DB___bam_rsplit:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is bam rsplit\n");
			}
			if ((ret = __bam_rsplit_read(dbenv, data, &rsplit_args)) != 0) {
				goto done;
			}

		    if (pgno == rsplit_args->pgno) {
				*prevPageLsn = LSN(rsplit_args->pgdbt.data);

			   if (DEBUG_PAGES) {
				   logmsg(LOGMSG_USER, "Rsplit: Page %d is non-root page\n", pgno);
			   }
			} else if(pgno == rsplit_args->root_pgno) {
				*prevPageLsn = rsplit_args->rootlsn;
				assert(LSN(rsplit_args->rootent.data) == *prevPageLsn);

			   if (DEBUG_PAGES) {
				   logmsg(LOGMSG_USER, "Rsplit: Page %d is root page\n", pgno);
			   }
			} else {
				// this should not happen.
			   if (DEBUG_PAGES) {
				   logmsg(LOGMSG_USER, "Rsplit: Page %d is not root page %d or non-root page %d\n", pgno, rsplit_args->root_pgno, rsplit_args->pgno);
			   }

			   ret = 1;
			   goto done;
			}

		   *apply = __bam_rsplit_recover;
		   *utxnid = rsplit_args->txnid->utxnid;
		   break;
		case DB___bam_repl:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is bam repl\n");
			}
		   if ((ret = __bam_repl_read(dbenv, data, &repl_args)) != 0) {
			   goto done;
		   }
		   *prevPageLsn = repl_args->lsn;
		   *apply = __bam_repl_recover;
		   *utxnid = repl_args->txnid->utxnid;
		   break;
		case DB___bam_adj:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is bam adj\n");
			}
		   if ((ret = __bam_adj_read(dbenv, data, &adj_args)) != 0) {
			   goto done;
		   }
		   *prevPageLsn = adj_args->lsn;
		   *apply = __bam_adj_recover;
		   *utxnid = adj_args->txnid->utxnid;
		   break;
		case DB___bam_cadjust:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is bam cadj\n");
			}
		   if ((ret = __bam_cadjust_read(dbenv, data, &cadjust_args)) != 0) {
			   goto done;
		   }
		   *prevPageLsn = cadjust_args->lsn;
		   *apply = __bam_cadjust_recover;
		   *utxnid = cadjust_args->txnid->utxnid;
		   break;
		case DB___bam_cdel: 
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is bam cdel\n");
			}
		   if ((ret = __bam_cdel_read(dbenv, data, &cdel_args)) != 0) {
			   goto done;
		   }
		   *prevPageLsn = cdel_args->lsn;
		   *apply = __bam_cdel_recover;
		   *utxnid = cdel_args->txnid->utxnid;
		   break;
		case DB___bam_prefix:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is bam prefix\n");
			}
		   if ((ret = __bam_prefix_read(dbenv, data, &prefix_args)) != 0) {
			   goto done;
		   }
		   *prevPageLsn = prefix_args->pagelsn;
		   *apply = __bam_prefix_recover;
		   *utxnid = prefix_args->txnid->utxnid;
		   break;
		case DB___db_pg_freedata:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is pg freedata\n");
			}
		   if ((ret = __db_pg_freedata_read(dbenv, data, &pg_freedata_args)) != 0) {
			   goto done;
		   }
		   *apply = __db_pg_freedata_recover;
		   *utxnid = pg_freedata_args->txnid->utxnid;
		   break;
		case DB___db_pg_free:
		   if ((ret = __db_pg_free_read(dbenv, data, &pg_free_args)) != 0) {
			   goto done;
		   }
		   *apply = __db_pg_free_recover;
		   *utxnid = pg_free_args->txnid->utxnid;
		   break;
			
		/*case DB___bam_pgcompact:
		   // TODO
		   ret = __bam_pgcompact_read(dbenv, data, &pgcompact_args);
		   if (ret) {
			   goto done;
		   }
		   *prevPageLsn = pgcompact_args->lsn;
		   *apply = __bam_pgcompact_recover;
		   *utxnid = pgcompact_args->txnid->txnid;
		   break;*/
		default:
			if (DEBUG_PAGES) {
				logmsg(LOGMSG_USER, "Op type of log record is unrecognized %d\n", rectype);
			}
			ret = 1;
			goto done;
			break;
	}
done:		
	return ret;
}

static int __mempv_evict_page_cache(mempv)
	DB_MEMPV *mempv;
{
	MEMPV_PAGE_CACHE *lru;

	int eviction = 0;
	while (!eviction) {
		lru = listc_rtl(&mempv->lru);
		if (lru == NULL) {
			return 1;
		}
		if (lru->pin == 1) {
			listc_abl(&mempv->lru, lru);
			continue;
		}

		hash_del(mempv->pages, lru);
		// destroy_pagelist(dbenv, lru);
		mspace_free(mempv->msp, lru);
		if (DEBUG_PAGES) {
			printf("%s: Evicted page from cache\n", __func__);
		}
		eviction = 1;
	}

	return 0;
}

int __mempv_add_page_header(mpf, dbp, hdr, pages)
	DB_MPOOLFILE *mpf;
	DB *dbp;
	MEMPV_PAGE_HEADER **hdr;
	MEMPV_PAGE_CACHE *pages;
{
	DB_ENV *dbenv;
	int ret;

	dbenv = mpf->dbenv;
	ret = 0;

	*hdr = mspace_malloc(dbenv->mempv->msp, offsetof(MEMPV_PAGE_HEADER, page) + offsetof(BH, buf) + dbp->pgsize);
	while (*hdr == NULL) {
		if (DEBUG_PAGES) {
			printf("%s: New hdr was null. Evicting page\n", __func__);
		}
		ret = __mempv_evict_page_cache(dbenv->mempv);
		if (ret) { goto done; }
		*hdr = mspace_malloc(dbenv->mempv->msp, offsetof(MEMPV_PAGE_HEADER, page) + offsetof(BH, buf) + dbp->pgsize);
	}

	(*hdr)->pagecache = pages;
	(*hdr)->commit_order.next = (*hdr)->commit_order.prev = NULL;
	LSN_NOT_LOGGED((*hdr)->prev_cache_entry_lsn);

done:
	return ret;
}

static void __mempv_print_page_cache(pages)
	MEMPV_PAGE_CACHE *pages;
{
	MEMPV_PAGE_HEADER *hdr;
	BH *bhp;
	PAGE * page_image;
	int num;

	hdr = pages->pages.top;
	num = 1;
	
	printf("==== Start of cache ====\n");

	while (hdr) {
		bhp = (BH *) (((char *) hdr) + offsetof(MEMPV_PAGE_HEADER, page));
		page_image = (PAGE *) (((char *) bhp) + offsetof(BH, buf));

		printf("--- hdr %d ---\n", num);
		printf("commit lsn: %d:%d\n", hdr->commit_lsn.file, hdr->commit_lsn.offset);
		printf("prev entry lsn: %d:%d\n", hdr->prev_cache_entry_lsn.file, hdr->prev_cache_entry_lsn.offset);
		printf("my lsn: %d:%d\n", LSN(page_image).file, LSN(page_image).offset);
		printf("--------------\n");

		hdr = hdr->commit_order.next;
		num++;
	}

	printf("==== End of cache ====\n");
}

static int __mempv_check_cache_for_version(pages, start_of_hole, end_of_hole, found, page_image, fetch_newest_version, target_lsn)
	MEMPV_PAGE_CACHE *pages;
	MEMPV_PAGE_HEADER **start_of_hole;
	MEMPV_PAGE_HEADER **end_of_hole;
	int *found;
	PAGE **page_image;
	int *fetch_newest_version;
	DB_LSN target_lsn;
{
	DB_LSN prev_cache_entry_lsn;
	MEMPV_PAGE_HEADER *hdr, *prev_hdr;
	int firstItr, ret;

	hdr = pages->pages.top;
	pages->pin = 1; // TODO: ?
	firstItr = 1;
	ret = 0;

	// __mempv_print_page_cache(pages);

	while (hdr) {
		if (DEBUG_PAGES) {
			printf("%s: Checking next cache header for match\n", __func__);
		}
		*page_image = (PAGE*) (hdr->page + offsetof(BH, buf));
		DB_LSN commit_lsn = hdr->commit_lsn;
		if (log_compare(&commit_lsn, &target_lsn) <= 0) {
			// Is target candidate. Now check for holes.
			if (firstItr) {
				if (pages->new_version == 1) {
					// The newest thing in the cache is older than us and there is a newer
					// version. We need to check it.
					if (DEBUG_PAGES)
						printf("Need to fetch new version.\n");
					pages->new_version = 0; // TODO: Consider if right place for this.
					*fetch_newest_version = 1;
					*end_of_hole = hdr;
					goto done;
				}
			} else if (log_compare(&hdr->prev_cache_entry_lsn, &prev_cache_entry_lsn) != 0) {
				// We found something older than us in the cache but there is a hole preceding it.
				// We need to make sure that the target version isn't in the hole.

				// Start walkback from preceding image.
				*end_of_hole = hdr;
				*start_of_hole = prev_hdr;

				if (DEBUG_PAGES)
					printf("Found a hole.\n");
				goto done;
			}

			// No holes. We found our target!
			// *(void **)ret_page = (void *) page_image;
			*found = 1;
			if (DEBUG_PAGES) {
				printf("%s: Found correct page version in cache. Version lsn %d:%d. Target lsn %d:%d\n", __func__, commit_lsn.file, commit_lsn.offset, target_lsn.file, target_lsn.offset);
			}

			goto done;
		}

		prev_hdr = hdr;
		prev_cache_entry_lsn = LSN(*page_image);
		firstItr = 0;

		hdr = hdr->commit_order.next;
	}

done:
	return ret;
}


/*
 * __mempv_fget --
 * 	Return a page in the version that it was at a past LSN.
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
	int have_lock, have_page, have_page_image, found, ret, fetch_newest_version;
	u_int64_t utxnid;
	u_int32_t rectype;
	DB_MEMPV *mempv;
	DB_MPOOL *dbmp;
	MEMPV_KEY key;
	MEMPV_PAGE_CACHE *pages;
	MEMPV_PAGE_HEADER *hdr, *prev_hdr, *start_of_hole, *end_of_hole;
	DB_LOGC *logc;
	PAGE *page, *page_image, *prev_page_image;
	DB_LSN curPageLsn, prevPageLsn, commit_lsn;
	DB_ENV *dbenv;
	BH *bhp, *prev_bhp;
	void *data_t;

	DBT dbt = {0};
	dbt.flags = DB_DBT_MALLOC;
	ret = 0;
	found = 0;
	fetch_newest_version = 0;
	logc = NULL;
	data_t = NULL;
	*(void **)ret_page = NULL;
	page_image = NULL;
	hdr = NULL;
	prev_hdr = NULL;
	start_of_hole = NULL;
	end_of_hole = NULL;
	dbenv = mpf->dbenv;
	mempv = dbenv->mempv;

	dbmp = dbenv->mp_handle;

	printf("dbmp %p dbenv %p mempv %p\n", dbmp, dbenv, mempv);

	// Get cached page versions. If DNE, create list of versions for this page.
	key.pgno = pgno;
	memcpy(key.ufid, mpf->fileid, DB_FILE_ID_LEN);
	Pthread_mutex_lock(&mempv->mempv_mutexp);
	pages = hash_find(mempv->pages, &key);
	int alloc_new_page_cache;
	if ((alloc_new_page_cache = pages == NULL)) {
		if (DEBUG_PAGES) {
			printf("%s: Creating page cache for key pgno %d ufid %s \n", __func__, pgno, key.ufid);
		}
		ret = __mempv_create_page_cache(dbp, pgno, &pages);
		if (ret) { goto done; }
	}

	// Check cache for correct version.

	__mempv_check_cache_for_version(pages, &start_of_hole, &end_of_hole, &found, &page_image, &fetch_newest_version, target_lsn);

	if (found) {
		goto rollback;
	}

	// If we are here then we are doing a rollback.
	// We either start from the page preceding a hole, or we start from the newest page (retrieved from memp_fget).

	if (fetch_newest_version || alloc_new_page_cache) {
		// Start from newest page.
		ret = __mempv_add_page_header(mpf, dbp, &hdr, pages);
		if (ret) {
			goto done;
		}

		bhp = (BH *) (((char *) hdr) + offsetof(MEMPV_PAGE_HEADER, page));
		page_image = (PAGE *) (((char *) bhp) + offsetof(BH, buf));

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

		if (alloc_new_page_cache) {
			pages->newest_lsn = LSN(page_image);
		}
		pages->new_version = 0;
		listc_atl(&pages->pages, hdr);
	} else {
		// Start from start of hole.
		hdr = start_of_hole;

		bhp = (BH *) (((char *) hdr) + offsetof(MEMPV_PAGE_HEADER, page));
		page_image = (PAGE *) (((char *) bhp) + offsetof(BH, buf));
	}

	// Rollback page to target version.

	if (IS_NOT_LOGGED_LSN(LSN(page_image))) {
		// assert((alloc_new_page_cache == 1) || (fetch_newest_version == 1));

		if (DEBUG_PAGES) {
			printf("%s: Original page has unlogged LSN\n", __func__);
		}
		LSN_NOT_LOGGED(hdr->commit_lsn); // TODO: ?
		found = 1;
		goto rollback;
	} else if ((ret = __log_cursor(dbenv, &logc)) != 0) {
		if (DEBUG_PAGES) {
			printf("%s: Failed to create log cursor\n", __func__);
		}
		ret = 1;
		goto done;
	}

rollback:
	while (!found) 
	{
		if (DEBUG_PAGES)
			printf("%s: Rolling back page %u with initial LSN %d:%d to prior LSN %d:%d\n", __func__, PGNO(page_image), LSN(page_image).file, LSN(page_image).offset, target_lsn.file, target_lsn.offset);

		if (IS_NOT_LOGGED_LSN(LSN(page_image))) {
			found = 1;
			break;
		}

		if (IS_ZERO_LSN(LSN(page_image))) {
			if (DEBUG_PAGES) {
				printf("%s: Got to page with zero / unlogged LSN\n", __func__);
			}
			ret = 1;
			goto done;
		}
		
		/* Move log cursor */
		ret = logc->get(logc, &LSN(page_image), &dbt, DB_SET);
		if (ret || (dbt.size < sizeof(int))) {
			if (DEBUG_PAGES) {
				printf("%s: Failed to get log cursor\n", __func__);
			}
			ret = 1;
			goto done;
		}

		// TODO: Encapsulate.
		LOGCOPY_32(&rectype, dbt.data);
		normalize_rectype(&rectype);
		if (rectype == DB___bam_split || rectype == DB___bam_rsplit || rectype == DB___db_pg_freedata || rectype == DB___db_pg_free ) {
			__os_malloc(dbenv, dbt.size, &data_t);
			memcpy(data_t, dbt.data, dbt.size);
		}
		if ((ret = __mempv_read_log_record(dbenv, data_t != NULL ? data_t : dbt.data, &apply, &prevPageLsn, &utxnid, PGNO(page_image))) != 0) {
			if (DEBUG_PAGES) {
				printf("%s: Failed to read log record\n", __func__);
			}
			ret = 1;
			goto done;
		}
		if (data_t != NULL) { 
			__os_free(dbenv, data_t);
			data_t = NULL;
	       	}

		/* TODO: Reword -- If the transaction that wrote this page is still in-progress or it committed before our target LSN, return this page. */
		if (((ret = __txn_commit_map_get(dbenv, utxnid, &commit_lsn)) == 1) || (log_compare(&commit_lsn, &target_lsn) <= 0)) {
			if (DEBUG_PAGES) {
				printf("%s: Found the right page version with lsn %d:%d and commit lsn %d:%d\n", __func__, LSN(page_image).file, LSN(page_image).offset, commit_lsn.file, commit_lsn.offset);
				if (ret) {
					printf("%s: Page commit LSN did not exist in the map\n", __func__);
				}
			}
			if (ret) {
				printf("%s: Page commit LSN did not exist in the map\n", __func__);
				LSN_NOT_LOGGED(hdr->commit_lsn); // TODO: ?
			} else {
				hdr->commit_lsn = commit_lsn; // TODO: Deal with commit lsn of things that don't exist. Should just be not logged?
			}
			found = 1;
			ret = 0;
			break;
		}

		// This will overwrite commit lsn of start_of_hole. Okay because it must have the same value.
		if (ret) {
			LSN_NOT_LOGGED(hdr->commit_lsn); // TODO: ?
		} else {
			hdr->commit_lsn = commit_lsn; // TODO: Deal with commit lsn of things that don't exist. Should just be not logged?
		}
		// create a new cache hdr for next page version.
		prev_hdr = hdr;
		prev_bhp = bhp;
		prev_page_image = page_image;
		curPageLsn = LSN(prev_page_image); // TODO: rename 'curPageLsn'

		ret = __mempv_add_page_header(mpf, dbp, &hdr, pages);
		if (ret) {
			goto done;
		}

		bhp = (BH *) (((char *) hdr) + offsetof(MEMPV_PAGE_HEADER, page));
		page_image = (PAGE *) (((char *) bhp) + offsetof(BH, buf));

		memcpy(bhp, prev_bhp, offsetof(BH, buf) + dbp->pgsize);

		if((ret = apply(dbenv, &dbt, &curPageLsn, DB_TXN_ABORT, page_image)) != 0) {
			if (DEBUG_PAGES) {
				printf("%s: Failed to undo log record\n", __func__);
			}
			ret = 1;
			goto done;
		}

		// Check if hole filled
		printf("end of hole %p lsn pg image %d:%d lsn end of hole %d:%d\n", end_of_hole, LSN(page_image).file, LSN(page_image).offset, end_of_hole != NULL ? LSN(end_of_hole).file : 0, end_of_hole != NULL ? LSN(end_of_hole).offset : 0);
		if ((end_of_hole != NULL) && (log_compare(&LSN(page_image), &LSN(end_of_hole->page + offsetof(BH, buf))) == 0)) {
			// We scanned a hole and did not find a newer version. Return end of hole. Throw out newly created header.

			assert (prev_hdr->commit_order.next == end_of_hole);
			assert (end_of_hole->commit_order.prev == prev_hdr);

			printf("%s: Closing hole\n", __func__);

			// Close the hole.

			/* Cache link */
			end_of_hole->prev_cache_entry_lsn = LSN(prev_page_image);

			// Throw out newly created header.
			mspace_free(mempv->msp, hdr);

			found = 1;
		} else {
			// Not end of a hole. Link in new header and continue scan.

			if (pages->pages.bot == prev_hdr) {
				// Add header to the bottom of the list.

				/* List link */
				listc_abl(&pages->pages, hdr);
			} else {
				// Add header somewhere within the list.
				assert (end_of_hole != NULL);

				/* List links */
				hdr->commit_order.next = end_of_hole;
				end_of_hole->commit_order.prev = hdr;

				hdr->commit_order.prev = prev_hdr;
				prev_hdr->commit_order.next = hdr;
			}

			/* Cache link */
			hdr->prev_cache_entry_lsn = LSN(prev_page_image);
		}
	}

	*(void **)ret_page = (void *) page_image;
	printf("fget pgno %d\n", PGNO(page_image));
done:
	if (logc)
		logc->close(logc, 0);
	if (dbt.data)
		__os_free(dbenv, dbt.data);
	if (ret) {
		pages->pin = 0;
	}
	Pthread_mutex_unlock(&mempv->mempv_mutexp);
	return ret;
}

/*
 * __mempv_fput --
 * 	Release a page accessed with __mempv_fget.
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
	MEMPV_KEY key;
	DB_MEMPV *mempv;
	MEMPV_PAGE_CACHE *pages;

	if (page == NULL) { return 0; }

	dbenv = mpf->dbenv;
	mempv = dbenv->mempv;
	key.pgno = PGNO(page);
	memcpy(key.ufid, mpf->fileid, DB_FILE_ID_LEN);
	Pthread_mutex_lock(&mempv->mempv_mutexp);
	pages = hash_find(mempv->pages, &key);

	if (DEBUG_PAGES)
		printf("Unpinning pgno %d ufid %s\n", key.pgno, key.ufid);
	pages->pin = 0;

	Pthread_mutex_unlock(&mempv->mempv_mutexp);
	return 0;
}
