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

int MEMPV_CACHE_ENTRY_NOT_FOUND = 1;
static int MAX_NUM_CACHED_PAGES = 50;
static int num_cached_pages = 0;

void __mempv_cache_dump(MEMPV_CACHE *cache);

int __mempv_cache_init(dbenv, cache, size)
	DB_ENV *dbenv;
	MEMPV_CACHE *cache;
	int size;
{
	int ret;

	ret = 0;

	cache->pages = hash_init_o(offsetof(MEMPV_CACHE_PAGE_VERSIONS, key), sizeof(MEMPV_CACHE_PAGE_KEY)); // A: init
	if (cache->pages == NULL) {
		ret = ENOMEM;
		goto done;
	}

	listc_init(&cache->evict_list, offsetof(MEMPV_CACHE_PAGE_HEADER, evict_link)); 
	pthread_rwlock_init(&(cache->lock), NULL);

done:
	return ret;
}

static int __mempv_cache_evict_page(dbp, cache, versions)
	DB *dbp;
	MEMPV_CACHE *cache;
	MEMPV_CACHE_PAGE_VERSIONS *versions;
{
	MEMPV_CACHE_PAGE_HEADER *to_evict;

	to_evict = listc_rtl(&cache->evict_list);
	if (to_evict == NULL) {
		return 1;
	}

	hash_del(to_evict->cache->versions, to_evict);
	if ((versions != to_evict->cache) && (hash_get_num_entries(to_evict->cache->versions) == 0)) {
		hash_del(cache->pages, to_evict->cache);
		hash_free(to_evict->cache->versions); 
		__os_free(dbp->dbenv, to_evict->cache); 
	}

	__os_free(dbp->dbenv, to_evict); 
	num_cached_pages--;
	
	return 0;
}

int __mempv_cache_put(dbp, cache, file_id, pgno, bhp, target_lsn)
	DB *dbp;
	MEMPV_CACHE *cache;
	u_int8_t file_id[DB_FILE_ID_LEN];
	db_pgno_t pgno;
	BH *bhp;
	DB_LSN target_lsn;
{
	MEMPV_CACHE_PAGE_VERSIONS *versions;
	MEMPV_CACHE_PAGE_KEY key;
	MEMPV_CACHE_PAGE_HEADER *page_header;
	int ret, allocd_versions, allocd_header;

	versions = NULL;
	page_header = NULL;
	ret = 0;
	allocd_versions = 0;
	allocd_header = 0;
	key.pgno = pgno;
	memcpy(key.ufid, file_id, DB_FILE_ID_LEN);

	pthread_rwlock_wrlock(&(cache->lock));

	PAGE *page_image = (PAGE *) (((u_int8_t *) bhp) + SSZA(BH, buf) );

	versions = hash_find(cache->pages, &key);
	if (versions != NULL) {
		goto put_version;
	}

create_new_cache:
	__os_malloc(dbp->dbenv, sizeof(MEMPV_CACHE_PAGE_VERSIONS), &versions); 
	if (versions == NULL) {
		ret = ENOMEM;
		goto err;
	}
	allocd_versions = 1;

	versions->key = key;
	versions->versions = hash_init_o(offsetof(MEMPV_CACHE_PAGE_HEADER, snapshot_lsn), sizeof(DB_LSN)); 
	if (versions->versions == NULL) {
		ret = ENOMEM;
		goto err;
	}


	ret = hash_add(cache->pages, versions);
	if (ret) {
		goto err;
	}

put_version:
	page_header = hash_find_readonly(versions->versions, &target_lsn);
	if (page_header != NULL) {
		goto done;
	}

	if(num_cached_pages == MAX_NUM_CACHED_PAGES) {
		if ((ret = __mempv_cache_evict_page(dbp, cache, versions)), ret != 0) {
			logmsg(LOGMSG_ERROR, "%s: Could not evict cache page\n", __func__);
			goto err;
		}
	}


	__os_malloc(dbp->dbenv, sizeof(MEMPV_CACHE_PAGE_HEADER)-sizeof(u_int8_t) + SSZA(BH, buf) + dbp->pgsize, &page_header); // E: Init
	if (page_header == NULL) {
		ret = ENOMEM;
		goto err;
	}
	allocd_header = 1;

	memcpy((char*)(page_header->page), bhp, offsetof(BH, buf) + dbp->pgsize);

	page_header->snapshot_lsn = target_lsn;
	page_header->cache = versions;
	listc_abl(&cache->evict_list, page_header);


	ret = hash_add(versions->versions, page_header);
	if (ret) {
		logmsg(LOGMSG_ERROR, "%s: Could not add entry to cache\n", __func__);
		goto err;
	}

	num_cached_pages++;

done:
	pthread_rwlock_unlock(&(cache->lock));
	return ret;
	
err:
	pthread_rwlock_unlock(&(cache->lock));

	return ret;
}

int __mempv_cache_get(dbp, cache, file_id, pgno, target_lsn, bhp)
	DB *dbp;
	MEMPV_CACHE *cache;
	u_int8_t file_id[DB_FILE_ID_LEN];
	db_pgno_t pgno;
	DB_LSN target_lsn;
	BH *bhp;
{
	MEMPV_CACHE_PAGE_VERSIONS *versions;
	MEMPV_CACHE_PAGE_KEY key;
	MEMPV_CACHE_PAGE_HEADER *page_header;
	int ret;
	u_int8_t cks;

	versions = NULL;
	page_header = NULL;
	ret = 0;
	key.pgno = pgno;
	memcpy(key.ufid, file_id, DB_FILE_ID_LEN);

	pthread_rwlock_wrlock(&(cache->lock));

	versions = hash_find_readonly(cache->pages, &key);
	if (versions == NULL) {
		ret = MEMPV_CACHE_ENTRY_NOT_FOUND; 
		goto done;
	}

	page_header = hash_find_readonly(versions->versions, &target_lsn);
	if (page_header == NULL) {
		ret = MEMPV_CACHE_ENTRY_NOT_FOUND;
		goto done;
	}

	/* Update LRU */
	listc_rfl(&cache->evict_list, page_header);
	listc_abl(&cache->evict_list, page_header);

	memcpy(bhp, (char*)(page_header->page), offsetof(BH, buf) + dbp->pgsize);
	PAGE *page_image = (PAGE *) (bhp + offsetof(BH, buf));

	page_image =(PAGE *) (page_header->page + offsetof(BH, buf));

done:
	pthread_rwlock_unlock(&(cache->lock));

	return ret;
}

static int __mempv_cache_page_version_dump(cache_page_version)
	MEMPV_CACHE_PAGE_HEADER *cache_page_version;
{
	printf("\n\t %p target lsn %d:%d\n", cache_page_version, cache_page_version->snapshot_lsn.file, cache_page_version->snapshot_lsn.offset);
	return 0;
}

static int __mempv_cache_page_dump(cache_page)
	MEMPV_CACHE_PAGE_VERSIONS *cache_page;
{
	printf("\n\n\t\tDUMPING PAGE %d ----\n", cache_page->key.pgno);
	hash_for(cache_page->versions, __mempv_cache_page_version_dump, NULL);
	printf("\n\n\t\tFINISHED DUMPING PAGE %d ----\n", cache_page->key.pgno);
	return 0;
}

void __mempv_cache_dump(cache)
	MEMPV_CACHE *cache;
{
	printf("DUMPING PAGE CACHE\n--------------------\n");
	hash_for(cache->pages, __mempv_cache_page_dump, NULL);
	printf("--------------------\nFINISHED DUMPING PAGE CACHE\n");
}
