#include "db_int.h"
#include "dbinc/mp.h"

// void *gbl_mempv_base = NULL;
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

	cache->pages = hash_init_o(offsetof(MEMPV_CACHE_PAGE_VERSIONS, key), sizeof(MEMPV_CACHE_PAGE_KEY));
	if (cache->pages == NULL) {
		ret = ENOMEM;
		goto done;
	}

	listc_init(&cache->evict_list, offsetof(MEMPV_CACHE_PAGE_HEADER, evict_link));

/*	gbl_mempv_base = malloc(size);
	if (!gbl_mempv_base) {
		ret = ENOMEM;
		goto done;
	}

	cache->msp = create_mspace_with_base(gbl_mempv_base, size, 1);*/

	pthread_rwlock_init(&(cache->lock), NULL);

done:
	return ret;
}

static MEMPV_CACHE_PAGE_HEADER* __mempv_cache_evict_page(dbp, cache, versions)
	DB *dbp;
	MEMPV_CACHE *cache;
	MEMPV_CACHE_PAGE_VERSIONS *versions;
{
	MEMPV_CACHE_PAGE_HEADER *to_evict;

	to_evict = listc_rtl(&cache->evict_list);
	if (to_evict == NULL) {
		printf("%s: No pages to evict\n", __func__);
		return NULL;
	}

	hash_del(to_evict->cache->versions, to_evict);
	if ((versions != to_evict->cache) && (hash_get_num_entries(to_evict->cache->versions) == 0)) {
		hash_free(to_evict->cache->versions);
		hash_del(cache->pages, to_evict->cache);
		__os_free(dbp->dbenv, to_evict->cache);
	}

	// mspace_free(cache->msp, to_evict);

	num_cached_pages--;
	
	return to_evict;
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
	int ret;

	versions = NULL;
	page_header = NULL;
	ret = 0;
	key.pgno = pgno;
	memcpy(key.ufid, file_id, DB_FILE_ID_LEN);

	pthread_rwlock_wrlock(&(cache->lock));

	versions = hash_find(cache->pages, &key);
	if (versions != NULL) {
		goto put_version;
	}

create_new_cache:
	ret = __os_malloc(dbp->dbenv, sizeof(MEMPV_CACHE_PAGE_VERSIONS), &versions);
	if (ret) {
		printf("%s: failed allocate page in cache\n", __func__);
		goto done;
	}

	versions->key = key;
	versions->versions = hash_init_o(offsetof(MEMPV_CACHE_PAGE_HEADER, snapshot_lsn), sizeof(DB_LSN));
	if (versions->versions == NULL) {
		printf("%s: failed allocate map for page in cache\n", __func__);
		ret = ENOMEM;
		goto done;
	}


	ret = hash_add(cache->pages, versions);
	if (ret) {
		printf("%s: failed to add page to cache\n", __func__);
		goto done;
	}

put_version:
	if(num_cached_pages == MAX_NUM_CACHED_PAGES) {
		// page_header = (MEMPV_CACHE_PAGE_HEADER *) mspace_malloc(cache->msp, offsetof(MEMPV_CACHE_PAGE_HEADER, page) + offsetof(BH, buf) + dbp->pgsize);

		page_header = __mempv_cache_evict_page(dbp, cache, versions);

		if (!page_header) {
			ret = 1;
			printf("%s: failed to evict header from cache\n", __func__);
			goto done;
		}
	} else {
		ret = __os_malloc(dbp->dbenv,offsetof(MEMPV_CACHE_PAGE_HEADER, page) + offsetof(BH, buf) + dbp->pgsize, &page_header);

		if (!page_header) {
			printf("%s: failed to allocate cache header\n", __func__);
			ret = ENOMEM;
			goto done;
		}
	}

	memcpy(((char *)(page_header->page)), bhp, offsetof(BH, buf) + dbp->pgsize);
	page_header->snapshot_lsn = target_lsn;
	page_header->cache = versions;
	listc_abl(&cache->evict_list, page_header);

	ret = hash_add(versions->versions, page_header);
	if (ret) {
		printf("%s: failed to add header to cache\n", __func__);
		goto done;
	}

	num_cached_pages++;

done:
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

	versions = NULL;
	page_header = NULL;
	ret = 0;
	key.pgno = pgno;
	memcpy(key.ufid, file_id, DB_FILE_ID_LEN);

	pthread_rwlock_rdlock(&(cache->lock));

	versions = hash_find_readonly(cache->pages, &key);
	if (versions == NULL) {
		ret = 1; // Better "not found" return code
		goto done;
	}

	page_header = hash_find_readonly(versions->versions, &target_lsn);
	if (page_header == NULL) {
		ret = 1;
		goto done;
	}

	memcpy(bhp, (char *)(page_header->page), offsetof(BH, buf) + dbp->pgsize);

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
