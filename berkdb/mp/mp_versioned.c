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

static int __mempv_page_version_is_guaranteed_target(dbenv, target_lsn, page_image)
    DB_ENV *dbenv;
    DB_LSN target_lsn;
    PAGE *page_image;
{
    return (IS_NOT_LOGGED_LSN(LSN(page_image)));
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
    dbt.flags = DB_DBT_MALLOC;
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

    while (!found) 
    {
        if (DEBUG_PAGES)
            printf("%s: Rolling back page %u with initial LSN %d:%d to prior LSN %d:%d\n", __func__, PGNO(page_image), LSN(page_image).file, LSN(page_image).offset, target_lsn.file, target_lsn.offset);

        if (__mempv_page_version_is_guaranteed_target(dbenv, target_lsn, page_image)) {
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

        /* If the transaction that wrote this page is still in-progress or it committed before our target LSN, return this page. */
        if (__txn_commit_map_get(dbenv, utxnid, &commit_lsn) || (log_compare(&commit_lsn, &target_lsn) <= 0)) {
            if (DEBUG_PAGES) {
                printf("%s: Found the right page version\n", __func__);
            }
            found = 1;
            ret = 0;
            break;
        }

        curPageLsn = LSN(page_image);
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
    if (logc)
        logc->close(logc, 0);
    if (dbt.data)
        __os_free(dbenv, dbt.data);
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
