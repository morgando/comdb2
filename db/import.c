/**
 * Bulk import allows you to quickly copy a table from a local copy of a db to
 * another copy of the db. This is useful when you have to replace a table's
 * contents (i.e. you get a new batch of information every month) but you don't
 * want to slow the main db down by adding all the new records and deleting the
 * old ones in a batch update.
 *
 * This is how it works:
 *
 * comdb2sc.tsk initiates the bulk import by opening an appsock_bulk_import
 * connection to the main db (the db who wants to import the contents of a
 * table), comdb2sc tells this main db the table to import and the name and
 * machine of the db to import from.
 *
 * The main db then opens a appsock_bulk_import_foreign connection to this other
 * "foreign" db and tells that db to stop it's threads, close the table (to
 * flush it) and then reply with its table's attributes and wait for a done
 * message.
 *
 * The main db then verifies that the foreign db's table's attributes (thus
 * 'guaranteeing' the files are compatible) match its own table's attributes and
 * kicks off a script to copy the table's data files to all of it's cluster
 * nodes (and perform some touch up like LSN resetting).
 *
 * After all the new data files are in place, the main db stops its own threads,
 * closes its table, update's its tables version numbers so they point to the
 * new files and reopens the table restarts its threads.
 *
 * The main db then tells the foreign db the import is complete and it can
 * reopen its table and resume its threads.
 */

#include <netdb.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "bdb_api.h"
#include "bdb_schemachange.h"
#include "comdb2.h"
#include "crc32c.h"
#include "intern_strings.h"
#include "logmsg.h"
#include "str0.h"
#include "comdb2_appsock.h"
#include "sc_global.h"
#include "sc_callbacks.h"
#include "importdata.pb-c.h"

extern char * gbl_import_src; // TODO pass as arg
extern char * gbl_dbname;
extern char * gbl_dbdir;
extern tran_type *curtran_gettran(void);
int bulk_import_data_load(ImportData *p_data);

/* Constants */
#define FILENAMELEN 100

/* Tunables */
int gbl_enable_bulk_import = 1;
int gbl_enable_bulk_import_different_tables;

extern char *gbl_import_table;

typedef struct {
    const char *src_name;
    const char *dest_name;
    int version; /* schema version */
    char *csc2[MAXVER];
    int odh;
    int compr;
    int compr_blob;
    int ipu;
    int isc;
    int dc_odh;
    int data_pgsz;
    int index_pgsz;
    int blob_pgsz;
} BulkImportMetaInfo;

int bulk_import_data_pack_to_file(ImportData *p_data, char *fname)
{
    int rc, bytes_written;
    FILE *f_bulk_import;
    (void)fname;
    // void *buf;
    ImportData import_data = IMPORT_DATA__INIT;
    import_data.table_name = strdup(gbl_import_table);
    import_data.n_index_genids = MAXINDEX;
    import_data.index_genids = malloc(sizeof(long unsigned int)*import_data.n_index_genids);
    import_data.n_blob_genids = MAXBLOBS;
    import_data.blob_genids = malloc(sizeof(long unsigned int)*import_data.n_blob_genids);
    bulk_import_data_load(&import_data);
   
    rc = bytes_written = 0;
    f_bulk_import = NULL;

    printf("about to open file %s\n", fname);
    f_bulk_import = fopen("bulk_import_data", "w");
    if (f_bulk_import == NULL) {
        logmsg(LOGMSG_ERROR, "Failed to open file");
        rc = 1;
        goto err;
    }

    printf("about to get packed size\n");
    unsigned len = import_data__get_packed_size(&import_data);
    void *buf = malloc(len);
    if (buf == NULL) {
        rc = ENOMEM;
        goto err;
    }

    printf("about to pack \n");
    if (import_data__pack(&import_data, buf) != len) {
        logmsg(LOGMSG_ERROR, "%s: Did not pack full protobuf buffer.\n", __func__);
        rc = 1;
        goto err;
    }

    printf("about to write %d serialized bytes\n", len);
    if (fwrite(buf, len, 1, f_bulk_import) != len) {
        logmsg(LOGMSG_ERROR, "%s: Did not write full protobuf buffer to file\n");
        rc = 1;
        goto err;
    }
    printf("about to done\n");

err:
    if (f_bulk_import != NULL) {
        fclose(f_bulk_import);
    }

    return rc;
}


int bulk_import_data_unpack_from_file(ImportData **pp_data, char *fname)
{
    void *line = NULL;
    FILE *fp = NULL;
    long fsize;
    int rc = 0;

    fp = fopen(fname, "r");
    if (!fp) {
        logmsg(LOGMSG_ERROR, "Failed to open file %s\n", fname);
        rc = 1;
        goto err;
    }

    fseek(fp, 0, SEEK_END);
    fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);  /* same as rewind(f); */

    line = malloc(fsize);
    if (!line) {
        logmsg(LOGMSG_ERROR, "Could not allocate line of size %ld\n", fsize+1);
        rc = ENOMEM;
        goto err;
    }

    size_t num_read = fread(line, fsize, 1, fp);
    if (num_read < 1) {
        logmsg(LOGMSG_ERROR, "Read less than expected from file %s. Num read = %lu and fsize = %lu\n", fname, num_read, fsize);
        goto err;
    }

    *pp_data = import_data__unpack(NULL, fsize, line);
    if (*pp_data == NULL) {
        logmsg(LOGMSG_ERROR, "Error unpacking incoming message\n");
        rc = 1;
        goto err;
    }

    // rc = bulk_import_data_unpack(&data, line);
    printf("Got this data from file %s\n", fname);
    // bulk_import_data_print(stdout, &data);

err:
    if (fp) {
        fclose(fp);
    }
    return rc;
}


/**
 * Prints all the bulk import data to specified file.
 * @param p_file    pointer to file to print to
 * @param p_data    pointer to data to print
 */
void bulk_import_data_print(FILE *p_file,
                                   const ImportData *p_data)
{
    unsigned i;
    unsigned j;

    fprintf(p_file,
            "table_name: %s data_dir: %s csc2_crc32:%x\n"
            "checksums: %d\n"
            "odh: %d compress: %d compress_blobs: %d\n"
            "dtastripe: %d blobstripe: %d\n"
            "data_genid: %"PRIx64" ",
            p_data->table_name, p_data->data_dir, p_data->csc2_crc32,
            p_data->checksums, p_data->odh, p_data->compress,
            p_data->compress_blobs, p_data->dtastripe, p_data->blobstripe,
            flibc_htonll(p_data->data_genid));
    if (p_data->filenames_provided) {
        for (j = 0; j < p_data->dtastripe; j++)
            fprintf(p_file, "%s ", p_data->data_files[j]);
    }
    fprintf(p_file,
            "\nnum_index_genids: %zu index_genids:", p_data->num_index_genids);
    for (i = 0; i < p_data->num_index_genids; ++i) {
        fprintf(p_file, " %"PRIx64" ", flibc_htonll(p_data->index_genids[i]));
        if (p_data->filenames_provided) {
            fprintf(p_file, "%s ", p_data->index_files[i]);
        }
    }
    fprintf(p_file,
            "\nnum_blob_genids: %zu blob_genids:", p_data->num_blob_genids);
    for (i = 0; i < p_data->num_blob_genids; ++i) {
        fprintf(p_file, " %llx ", (long long unsigned int) p_data->blob_genids[i]);
        if (p_data->filenames_provided) {
            if (p_data->blobstripe) {
                for (j = 0; j < p_data->dtastripe; j++)
                    fprintf(p_file, "%s ", p_data->blob_files[i]->files[j]);
            } else
                fprintf(p_file, "%s ", p_data->blob_files[i]->files[0]);
        }
    }
    fprintf(p_file, "\n");
}

/**
 * Clear the strdups in this structures
 *
 * @param p_data    pointer to place that stores bulk import data,
 */
static void clear_bulk_import_data(ImportData *p_data)
{
    if (p_data->index_genids) {
        free(p_data->index_genids);
    }
    if (p_data->blob_genids) {
        free(p_data->blob_genids);
    }
    if (p_data->table_name) {
        free(p_data->table_name);
    }
    if (p_data->data_dir) {
        free(p_data->data_dir);
    }
    for (int i=0; i<p_data->n_data_files; ++i) {
        free(p_data->data_files[i]);
    }
    if (p_data->data_files) {
        free(p_data->data_files);
    }
    for (int i=0; i<p_data->n_index_files; ++i) {
        free(p_data->index_files[i]);
    }
    if (p_data->index_files) {
        free(p_data->index_files);
    }
    for (int i=0; i<p_data->n_blob_files; ++i) {
        BlobFiles *b = p_data->blob_files[i];
        for (int j=0; j<b->n_files; j++) {
            free(b->files[j]);
        }
        if (b->files) {
            free(b->files);
        }
        free(b);
    }
    if (p_data->blob_files) {
        free(p_data->blob_files);
    }
}

/**
 * Grabs all the bulk import data for the local table specified in
 * p_data->table_name.
 * @param p_data    pointer to place to store bulk import data, the table_name
 *                  member should be set as input
 * @return 0 on success !0 otherwise
 */
int bulk_import_data_load(ImportData *p_data)
{
    unsigned i, j;
    int bdberr;
    struct dbtable *db;
    char *p_csc2_text = NULL;
    char tempname[64 /*hah*/];
    int len, pgsz;

    /* clear data that may not get initialized*/
    p_data->compress = 0;
    p_data->compress_blobs = 0;

    /* find the table we're using */
    if (!(db = get_dbtable_by_name(p_data->table_name))) {
        logmsg(LOGMSG_ERROR, "%s: no such table: %s\n", __func__,
               p_data->table_name);
        return -1;
    }

    p_data->filenames_provided = gbl_enable_bulk_import_different_tables;

    /* get the data dir */
    if (strlen(thedb->basedir) >= 10000 /*placeholder */) {
        logmsg(LOGMSG_ERROR, "%s: basedir too large: %s\n", __func__,
               thedb->basedir);
        return -1;
    }
    p_data->data_dir = strdup(thedb->basedir);

    /* get table's schema from the meta table and calculate it's crc32 */
    if (get_csc2_file(p_data->table_name, -1 /*highest csc2_version*/,
                      &p_csc2_text, NULL /*csc2len*/)) {
        logmsg(LOGMSG_ERROR, "%s: could not get schema for table: %s\n",
               __func__, p_data->table_name);
        return -1;
    }

    p_data->csc2_crc32 = crc32c((const void *)p_csc2_text, strlen(p_csc2_text));
    free(p_csc2_text);
    p_csc2_text = NULL;

    /* get checksums option */
    p_data->checksums = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_CHECKSUMS);

    /* get odh options from the meta table */
    if (get_db_odh(db, &p_data->odh) ||
        (p_data->odh && (get_db_compress(db, &p_data->compress) ||
                         get_db_compress_blobs(db, &p_data->compress_blobs)))) {
        logmsg(LOGMSG_ERROR, "%s: failed to fetch odh flags for table: %s\n",
               __func__, p_data->table_name);
        return -1;
    }

    /* get stripe options */
    p_data->dtastripe = gbl_dtastripe;
    p_data->blobstripe = gbl_blobstripe;

    // p_data->n_index_files = p_data->n_index_genids;
    // p_data->index_files = malloc(sizeof(char *)*p_data->n_index_files);
    // todo


    /* get data file's current version from meta table */
    if (bdb_get_file_version_data(db->handle, NULL /*tran*/, 0 /*dtanum*/,
                                  (unsigned long long *) &p_data->data_genid, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to fetch version number for %s's main data "
               "files\n",
               __func__, p_data->table_name);
        return -1;
    }

    /* get page sizes from meta table */
    /*
     * Don't know if this is necessary.
     if (bdb_get_pagesize_data(db->handle, NULL, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        p_data->data_pgsz = pgsz;
    } else {
        logmsg(LOGMSG_ERROR, "%s: Failed to fetch data pagesize for table %s with bdberr %d\n",
                __func__, p_data->table_name, bdberr);
        return -1;
    }
    if (bdb_get_pagesize_index(db->handle, NULL, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        p_data->index_pgsz = pgsz;
    } else {
        logmsg(LOGMSG_ERROR, "%s: Failed to fetch index pagesize for table %s with bdberr %d\n",
                __func__, p_data->table_name, bdberr);
        return -1;
    }
    if (bdb_get_pagesize_blob(db->handle, NULL, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        p_data->blob_pgsz = pgsz;
    } else {
        logmsg(LOGMSG_ERROR, "%s: Failed to fetch blob pagesize for table %s with bdberr %d\n",
                __func__, p_data->table_name, bdberr);
        return -1;
    }*/

    /* get ipu/isc options from meta table */

    if (get_db_inplace_updates(db, &p_data->ipu)) {
        logmsg(LOGMSG_ERROR, "%s: Failed to get inplace update option for table %s\n",
               __func__, p_data->table_name);
    }
    if (get_db_instant_schema_change(db, &p_data->isc)) {
        logmsg(LOGMSG_ERROR, "%s: Failed to get instant schema change option for table %s\n",
               __func__, p_data->table_name);
    }
    if (get_db_datacopy_odh(db, &p_data->dc_odh)) {
        logmsg(LOGMSG_ERROR, "%s: Failed to get datacopy odh option for table %s\n",
               __func__, p_data->table_name);
    }

    if (gbl_enable_bulk_import_different_tables) {
        p_data->n_data_files = p_data->dtastripe;
        p_data->data_files = malloc(sizeof(char *)*p_data->n_data_files);

        for (i = 0; i < p_data->dtastripe; i++) {
            len = bdb_form_file_name(db->handle, 1, 0, i, p_data->data_genid,
                                     tempname, sizeof(tempname));
            if (len <= 0 || len > 64) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to retrieve the data filename, stripe %d\n",
                       __func__, i);
            }
            p_data->data_files[i] = strdup(tempname);
        }
    }
    //

    // create t
    tran_type *t = curtran_gettran();
    int version = get_csc2_version_tran(p_data->table_name, t);
        if (version == -1) {
            logmsg(LOGMSG_ERROR, "%s: Could not find csc2 version for table %s\n", __func__, p_data->table_name);
            return 1;
        }

    p_data->n_csc2 = version;
    p_data->csc2 = malloc(sizeof(char *)*p_data->n_csc2);

    for (int vers=1; vers<=version; vers++) {
        get_csc2_file_tran(p_data->table_name, vers, &p_data->csc2[vers-1], &len, t);
        printf("loading csc2 at %d:\n %s\n", vers, p_data->csc2[vers-1]);
    }

    /* get num indicies/blobs */
    p_data->num_index_genids = db->nix;
    p_data->n_index_genids = p_data->num_index_genids;
    p_data->index_genids = malloc(sizeof(unsigned long int)*p_data->n_index_genids);
    p_data->num_blob_genids = db->numblobs;
    p_data->n_blob_genids = p_data->num_blob_genids;
    p_data->blob_genids = malloc(sizeof(unsigned long int)*p_data->n_blob_genids);

    /* for each index, lookup version */
    for (i = 0; i < p_data->num_index_genids; ++i) {
        /* get index file's current version from meta table */
        if (bdb_get_file_version_index(db->handle, NULL /*tran*/, i /*ixnum*/,
                                       (unsigned long long *) &p_data->index_genids[i], &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to fetch version number for %s's "
                   "index: %d files\n",
                   __func__, p_data->table_name, i);
            return -1;
        }

        if (gbl_enable_bulk_import_different_tables) {
            len =
                bdb_form_file_name(db->handle, 0, 0, i, p_data->index_genids[i],
                                   tempname, sizeof(tempname));
            if (len <= 0 || len > 64) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to retrieve the index filename, ix %d\n",
                       __func__, i);
            }
            p_data->index_files[i] = strdup(tempname);
        }
    }

    if (gbl_enable_bulk_import_different_tables && p_data->num_blob_genids > 0) {
        p_data->n_blob_files = p_data->dtastripe;
        p_data->blob_files = malloc(sizeof(BlobFiles *)*p_data->n_blob_files);
        for (int i=0; i<p_data->n_blob_files; ++i) {
            p_data->blob_files[i] = malloc(sizeof(BlobFiles));

            BlobFiles *b = p_data->blob_files[i];
            b->n_files = p_data->dtastripe;
            b->files = malloc(sizeof(char *)*b->n_files);
        }
    }

    /* for each blob, lookup and compare versions */
    for (i = 0; i < p_data->num_blob_genids; ++i) {
        /* get blob file's current version from meta table */
        if (bdb_get_file_version_data(db->handle, NULL /*tran*/,
                                      i + 1 /*dtanum*/, (unsigned long long *) &p_data->blob_genids[i],
                                      &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to fetch version number for %s's "
                   "blob: %d files\n",
                   __func__, p_data->table_name, i);
            return -1;
        }

        if (gbl_enable_bulk_import_different_tables) {
            if (p_data->blobstripe) {
                for (j = 0; j < p_data->dtastripe; j++) {
                    len = bdb_form_file_name(db->handle, 1, i + 1, j,
                                             p_data->blob_genids[i], tempname,
                                             sizeof(tempname));
                    if (len <= 0 || len > 64) {
                        logmsg(LOGMSG_ERROR,
                               "%s: failed to retrieve the blob "
                               "filename, ix %d stripe %d\n",
                               __func__, i, j);
                    }
                    p_data->blob_files[i]->files[j] = strdup(tempname);
                }
            } else {
                len = bdb_form_file_name(db->handle, 1, i + 1, 0,
                                         p_data->blob_genids[i], tempname,
                                         sizeof(tempname));
                if (len <= 0 || len > 64) {
                    logmsg(LOGMSG_ERROR,
                           "%s: failed to retrieve the blob filename, "
                           "ix %d stripe %d\n",
                           __func__, i, 0);
                }
                p_data->blob_files[i]->files[0] = strdup(tempname);
            }
        }
    }

    /* success */
    return 0;
}

/**
 * Performs all the checks necessary to make sure that we are capable of doing a
 * bulk import and also validates the foreign data we recieved to make sure it's
 * compatible with our local data.
 * @param p_local_data  pointer to our local table's data and settings
 * @param p_foreign_data    pointer to the foreign table's data and settings to
 *                          validate
 * @return 0 on success, !0 otherwise
 */
static int
bulk_import_data_validate(const ImportData *p_local_data,
                          const ImportData *p_foreign_data,
                          unsigned long long dst_data_genid,
                          const unsigned long long *p_dst_index_genids,
                          const unsigned long long *p_dst_blob_genids)
{
    /* lots of sanity checks so that hoefully we never swap in incompatible data
     * files */

    if (!gbl_enable_bulk_import) {
        logmsg(LOGMSG_ERROR, "%s: bulk imports not enabled\n", __func__);
        return -1;
    }

    if (thedb->master != gbl_myhostname) {
        logmsg(LOGMSG_ERROR, "%s: I'm not the master\n", __func__);
        return -1;
    }

    /* if we don't have the filenames, we enforce same tablename rule */
    if (!p_foreign_data->filenames_provided) {
        /* compare table name */
        if (strcmp(p_local_data->table_name, p_foreign_data->table_name)) {
            logmsg(LOGMSG_ERROR, "%s: table names differ: %s %s\n", __func__,
                   p_local_data->table_name, p_foreign_data->table_name);
            fprintf(
                stderr,
                "%s: check that both servers support passing files by names\n",
                __func__);
            return -1;
        }
    }

    /* do not check data_dir for equality since it doesn't need to be the same
     * on all machines */

    unsigned long long genid;
    struct dbtable *db = get_dbtable_by_name(p_local_data->table_name);
    if (get_blobstripe_genid(db, &genid) == 0) {
        fprintf(
            stderr,
            "Destination database has a blobstripe genid. Can't bulkimport\n");
        return -1;
    }

    /* compare stripe options */
    if (p_local_data->dtastripe != p_foreign_data->dtastripe ||
        p_local_data->blobstripe != p_foreign_data->blobstripe) {
        logmsg(LOGMSG_ERROR,
               "%s: stripe settings differ for table: %s dtastripe: "
               "%d %d blobstripe: %d %d\n",
               __func__, p_local_data->table_name, p_local_data->dtastripe,
               p_foreign_data->dtastripe, p_local_data->blobstripe,
               p_foreign_data->blobstripe);
        return -1;
    }

    /* compare checksums option */
    if (p_local_data->checksums != p_foreign_data->checksums) {
        logmsg(LOGMSG_ERROR, "%s: %s's checksums settings differ: %d %d\n",
               __func__, p_local_data->table_name, p_local_data->checksums,
               p_foreign_data->checksums);
        return -1;
    }

    /* success */
    return 0;
}

/**
 * Gives the filenames associated with the target table on the foregin db
 * and the new filenames where the table will live on the local db.
 * 
 * @param p_data               pointer to struct containing all the local db's
 *                              table's attributes
 * @param p_foreign_data        pointer to struct containing all the foreign db's
 *                              table's attributes
 * @param dst_data_genid        data genid on local db.
 * @param p_dst_index_genids    pointer to index genids on local db.
 * @param p_dst_blob_genids     pointer to blob genids on local db.
 * @param p_src_files           pointer to a list of foreign files to be populated.
 * @param p_dst_files           pointer to a list of local files to be populated.
 * @param num_files             pointer to number of files populated in each list.
 * @param bdberr                bdb error, if any
 *
 * @return 0 on success !0 otherwise
 */
static int bulk_import_generate_filenames(
    const ImportData *p_data, const ImportData *p_foreign_data,
    const unsigned long long dst_data_genid,
    const unsigned long long *p_dst_index_genids,
    const unsigned long long *p_dst_blob_genids,
    char ***p_src_files,
    char ***p_dst_files,
    int *p_num_files,
    int *bdberr)
{
    struct dbtable *db = NULL;
    int dtanum;
    int ixnum;
    int fileix=0;

    *p_num_files = p_foreign_data->num_index_genids + (p_foreign_data->blobstripe ? p_foreign_data->num_blob_genids*p_data->dtastripe : p_foreign_data->num_blob_genids) + p_data->dtastripe;
    *p_src_files = malloc(sizeof(char *)*(*p_num_files));
    *p_dst_files = malloc(sizeof(char *)*(*p_num_files));
    for (int i=0; i<(*p_num_files); ++i) {
        (*p_src_files)[i] = (char *) malloc(FILENAMELEN);
        (*p_dst_files)[i] = (char *) malloc(FILENAMELEN);
    }

    char **src_files = *p_src_files;
    char **dst_files = *p_dst_files;

    /* find the table we're importing TO */
    if (!(db = get_dbtable_by_name(p_data->table_name))) {
        logmsg(LOGMSG_ERROR, "%s: no such table: %s\n", __func__,
               p_data->table_name);
        return -1;
    }

    /* add data files */
    for (dtanum = 0; dtanum < p_foreign_data->num_blob_genids + 1; dtanum++) {
        int strnum;
        int num_stripes = 0;
        unsigned long long src_version_num;
        unsigned long long dst_version_num;

        if (dtanum == 0) {
            num_stripes = p_data->dtastripe;
            src_version_num = p_foreign_data->data_genid;
            dst_version_num = dst_data_genid;
        } else {
            if (p_data->blobstripe) {
                num_stripes = p_data->dtastripe;
            } else {
                num_stripes = 1;
            }
            src_version_num = p_foreign_data->blob_genids[dtanum - 1];
            dst_version_num = p_dst_blob_genids[dtanum - 1];
        }

        /* for each stripe add a -file param */
        for (strnum = 0; strnum < num_stripes; ++strnum) {
            /* add src filename */
            if (p_foreign_data->filenames_provided) {
                if (dtanum == 0) {
                    strcpy(src_files[fileix], p_foreign_data->data_files[strnum]);
                } else {
                    strcpy(src_files[fileix], p_foreign_data->blob_files[dtanum - 1]->files[strnum]);
                }
            } else {
                bdb_form_file_name(db->handle, 1 /*is_data_file*/, dtanum,
                                         strnum, src_version_num,
                                         src_files[fileix], FILENAMELEN);
            }

            /* add dst filename */
            bdb_form_file_name(db->handle, 1 /*is_data_file*/, dtanum,
                                     strnum, dst_version_num, dst_files[fileix],
                                     FILENAMELEN);
            fileix++;
        }
    }

    /* for each index add a -file param */
    for (ixnum = 0; ixnum < p_foreign_data->num_index_genids; ixnum++) {
        /* add src filename */
        if (p_foreign_data->filenames_provided) {
            strcpy(src_files[fileix], p_foreign_data->index_files[ixnum]);
        } else {
            bdb_form_file_name(db->handle, 0 /*is_data_file*/, ixnum,
                                 0 /*strnum*/, p_foreign_data->index_genids[ixnum],
                                 src_files[ixnum], FILENAMELEN);
        }

        /* add dst filename */
        bdb_form_file_name(db->handle, 0 /*is_data_file*/, ixnum,
                                 0 /*strnum*/, p_dst_index_genids[ixnum],
                                 dst_files[fileix], FILENAMELEN);

        fileix++;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

static int bulkimport_switch_files(struct dbtable *db,
                                   const ImportData *p_foreign_data,
                                   unsigned long long dst_data_genid,
                                   unsigned long long *dst_index_genids,
                                   unsigned long long *dst_blob_genids,
                                   BulkImportMetaInfo *info,
                                   ImportData *local_data)
{
    int i, outrc, bdberr;
    int retries = 0;
    tran_type *tran = NULL;
    struct ireq iq;

    init_fake_ireq(thedb, &iq);
    iq.usedb = db;

    /* stop the db */
    void *lock_table_tran = bdb_tran_begin_logical(db->handle, 0, &bdberr);
    assert(lock_table_tran);
    bdb_lock_table_write(db->handle, lock_table_tran);

    /* close the table */
    if (bdb_close_only(db->handle, &bdberr)) {
        logmsg(LOGMSG_ERROR, "%s: failed to close table: %s bdberr: %d\n",
               __func__, p_foreign_data->table_name, bdberr);
        bdb_tran_abort(thedb->bdb_env, lock_table_tran, &bdberr);
        return -1;
    }

    /* from here on use goto backout not return */
    llmeta_dump_mapping_table(thedb, db->tablename, 1);

retry_bulk_update:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__,
               retries);

        outrc = -1;
        goto backout;
    }

    if (tran) /* if this is a retry and not the first pass */
    {
        trans_abort(&iq, tran);
        tran = NULL;

        logmsg(LOGMSG_ERROR,
               "%s: bulk update failed for table: %s attempting "
               "retry\n",
               __func__, p_foreign_data->table_name);
    }

    if (trans_start(&iq, NULL /*parent_trans*/, &tran)) {
        logmsg(LOGMSG_ERROR,
               "%s: failed starting bulk update transaction for "
               "table: %s\n",
               __func__, p_foreign_data->table_name);
        goto retry_bulk_update;
    }

    /* update version for main data files */
    if (bdb_new_file_version_data(db->handle, tran, 0 /*dtanum*/,
                                  dst_data_genid, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "%s: failed updating version for table: %s main data "
               "files\n",
               __func__, p_foreign_data->table_name);
        goto retry_bulk_update;
    }

    /* for each index, update version */
    for (i = 0; i < p_foreign_data->num_index_genids; ++i) {
        /* update version for index */
        if (bdb_new_file_version_index(db->handle, tran, i /*ixnum*/,
                                       dst_index_genids[i], &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed updating version for %s's index: %d "
                   "files new version: %llx\n",
                   __func__, p_foreign_data->table_name, i,
                   (long long unsigned int) p_foreign_data->index_genids[i]);
            goto retry_bulk_update;
        }
    }

    /* for each blob, update version */
    for (i = 0; i < p_foreign_data->num_blob_genids; ++i) {
        /* update version for index */
        if (bdb_new_file_version_data(db->handle, tran, i + 1 /*dtanum*/,
                                      dst_blob_genids[i], &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed updating version for %s's blob: %d "
                   "files new version: %llx\n",
                   __func__, p_foreign_data->table_name, i,
                   (long long unsigned int) p_foreign_data->blob_genids[i]);
            goto retry_bulk_update;
        }
    }

    if (table_version_upsert(db, tran, &bdberr) || bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s: failed to upsert table version bdberr %d\n",
               __func__, bdberr);
        goto retry_bulk_update;
    }

    bdb_reset_csc2_version(tran, db->tablename, db->schema_version, 1);
    put_db_odh(db, tran, p_foreign_data->odh);
    put_db_compress(db, tran, p_foreign_data->compress);
    put_db_compress_blobs(db, tran, p_foreign_data->compress_blobs);
    put_db_inplace_updates(db, tran, info->ipu);
    put_db_instant_schema_change(db, tran, info->isc);
    put_db_datacopy_odh(db, tran, info->dc_odh);
    for (i = 1; i <= p_foreign_data->n_csc2; ++i) {
        printf("csc2 version %d is:\n %s\n", i, p_foreign_data->csc2[i-1]);
        put_csc2_file(db->tablename, tran, i, p_foreign_data->csc2[i-1]);
    }
    // bdb_set_pagesize_data(db->handle, tran, info->data_pgsz, &bdberr);
    // bdb_set_pagesize_index(db->handle, tran, info->index_pgsz, &bdberr);
    // bdb_set_pagesize_blob(db->handle, tran, info->blob_pgsz, &bdberr);

    /* commit new versions */
    if (trans_commit_adaptive(&iq, tran, gbl_myhostname)) {
        logmsg(LOGMSG_ERROR, "%s: failed bulk update commit for table: %s\n",
               __func__, p_foreign_data->table_name);
        goto retry_bulk_update;
    }

    if (reload_after_bulkimport(db, NULL)) {
        /* There is no good way to rollback here. The new schema's were
         * committed but we couldn't reload them (parse error?). Lets just
         * abort here and hope we can do this after bounce */
        logmsg(LOGMSG_ERROR, "%s: failed reopening table: %s\n", __func__,
               local_data->table_name);
        clean_exit();
    }
    bdb_tran_abort(thedb->bdb_env, lock_table_tran, &bdberr);
    llmeta_dump_mapping_table(thedb, db->tablename, 1 /*err*/);
    sc_del_unused_files(db);
    int rc = bdb_llog_scdone(thedb->bdb_env, bulkimport, db->tablename,
                             strlen(db->tablename) + 1, 1, &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        /* TODO: there is no way out as llmeta was committed already */
        logmsg(LOGMSG_ERROR,
               "%s: failed to send logical log scdone for table: %s "
               "bdberr: %d\n",
               __func__, p_foreign_data->table_name, bdberr);
    }
    return 0;

backout:
    llmeta_dump_mapping_table(thedb, db->tablename, 1 /*err*/);

    /* free the old bdb handle */
    if (bdb_free(db->handle, &bdberr) || bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "%s: failed freeing old db for table: %s bdberr %d\n", __func__,
               p_foreign_data->table_name, bdberr);
        clean_exit();
    }

    /* open the table again, we use bdb_open_more() not bdb_open_again() because
     * the underlying files changed */
    if (!(db->handle = bdb_open_more(
              local_data->table_name, thedb->basedir, 0, db->nix, (short*)db->ix_keylen,
              db->ix_dupes, db->ix_recnums, db->ix_datacopy, db->ix_datacopylen, db->ix_collattr,
              db->ix_nullsallowed, db->numblobs + 1 /*main dta*/,
              thedb->bdb_env, &bdberr)) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s: failed reopening table: %s, bdberr %d\n",
               __func__, local_data->table_name, bdberr);
        clean_exit();
    }

    bdb_tran_abort(thedb->bdb_env, lock_table_tran, &bdberr);

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DELAYED_OLDFILE_CLEANUP)) {
        /* delete files we don't need now */
        if (bdb_list_unused_files(db->handle, &bdberr, "bulkimport") ||
            bdberr != BDBERR_NOERROR)
            logmsg(LOGMSG_ERROR,
                   "%s: errors deleting files for table: %s bdberr: %d\n",
                   __func__, local_data->table_name, bdberr);
    } else {
        /* delete files we don't need now */
        if (bdb_del_unused_files(db->handle, &bdberr) ||
            bdberr != BDBERR_NOERROR)
            logmsg(LOGMSG_ERROR,
                   "%s: errors deleting files for table: %s bdberr: %d\n",
                   __func__, local_data->table_name, bdberr);
    }

    /* if we were successful */
    if (!outrc)
        printf("%s: successful for table: %s\n", __func__,
               local_data->table_name);

    clear_bulk_import_data(local_data);

    return outrc;
}

int bulk_import_tmpdb_copy_and_recover()
{
    int rc, f;
    char *fname, *nextFname;
    char txndir[2*strlen(gbl_dbdir) + strlen("/.txn")];
    char query[2000];

    rc = 0;
    f = -1;
    fname = nextFname = NULL;

    // GET HANDLE TO SOURCE

    cdb2_hndl_tp *hndl;
    rc = cdb2_open(&hndl, gbl_import_src, "local", 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Could not open a handle to src db in import mode\n", __func__);
        rc = 1;
        goto err;
    }

    logmsg(LOGMSG_DEBUG, "[IMPORT] %s: Got cdb2api handle to source db\n", __func__);

    // FLUSH SOURCE

    snprintf(query, sizeof(query), "exec procedure sys.cmd.send('flush')");
    rc = cdb2_run_statement(hndl, query);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Got an error flushing src db. errstr: %s\n", cdb2_errstr(hndl));
        rc = 1;
        goto err;
    }
    while(cdb2_next_record(hndl) == CDB2_OK) {}

    logmsg(LOGMSG_DEBUG, "[IMPORT] %s: Flushed source db\n", __func__);

    // GET DBS + LOGS

    snprintf(query, sizeof(query), "SELECT filename, content, dir FROM comdb2_files WHERE dir!='tmp' ORDER BY filename, offset");
    rc = cdb2_run_statement(hndl, query);

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Got an error grabbing files from src db. errstr: %s\n", cdb2_errstr(hndl));
        rc = 1;
        goto err;
    }



    if (gbl_nonames)
        snprintf(txndir, sizeof(txndir), "%s/logs", gbl_dbdir);
    else
        snprintf(txndir, sizeof(txndir), "%s/%s.txn", gbl_dbdir, gbl_dbname);

    while(cdb2_next_record(hndl) == CDB2_OK) {
        nextFname = (char *) cdb2_column_value(hndl, 0);
        if (strcmp(nextFname, "checkpoint") == 0) {
            continue;
        }
        int newFile = fname == NULL || strcmp(fname, nextFname) != 0;
        if (newFile) {
            if (fname != NULL) {
                free(fname);
            }
            if (f != -1) { close(f); }

            fname = strdup(nextFname);

            char copy_dst[strlen(gbl_dbdir) + 2 + cdb2_column_size(hndl, 2) + strlen(fname)];
            snprintf(copy_dst, sizeof(copy_dst), "%s%s%s%s%s", gbl_dbdir, cdb2_column_size(hndl, 2) != 1 ? "/" : "", (char *) cdb2_column_value(hndl, 2), "/", fname);

            f = open(copy_dst, O_WRONLY | O_CREAT | O_APPEND, 0755);
            if (f  == -1) {
                logmsg(LOGMSG_ERROR, "failed to open file %s (errno: %s)\n", copy_dst, strerror(errno));
                rc = 1;
                goto err;
            }
        }
        logmsg(LOGMSG_DEBUG, "[IMPORT] %s: Writing chunk to file %s\n", __func__, fname);
        ssize_t bytes_written = write(f, (char *) cdb2_column_value(hndl, 1), cdb2_column_size(hndl, 1));
        if (bytes_written != cdb2_column_size(hndl, 1)) {
            logmsg(LOGMSG_ERROR, "failed to write to the file (expected: %d got: %ld)\n",
                  cdb2_column_size(hndl, 1), bytes_written);
            rc = 1;
            goto err;
        }
    }



err:
    if (f != -1) { close(f); }

    if (fname != NULL) { free(fname); }

    return rc;
}

int bulk_import_v2(ImportData *p_foreign_data)
{
    unsigned i;
    int offset, num_files, bdberr, rc;
    struct dbtable *db;
    unsigned long long dst_data_genid;
    unsigned long long dst_index_genids[MAXINDEX];
    unsigned long long dst_blob_genids[MAXBLOBS];
    ImportData local_data = IMPORT_DATA__INIT;
    BulkImportMetaInfo info = {0};
    char ** src_files = NULL;
    char ** dst_files = NULL;

    rc = num_files = 0;

    // get local data 
    local_data.table_name = strdup(p_foreign_data->table_name);
    if (bulk_import_data_load(&local_data)) {
        logmsg(LOGMSG_ERROR, "%s: failed getting local data\n", __func__);
        rc = -1;
        return rc;
    }

    logmsg(LOGMSG_DEBUG, "%s: Loaded local import data\n", __func__);

    // find the table we're importing 
    if (!(db = get_dbtable_by_name(local_data.table_name))) {
        logmsg(LOGMSG_ERROR, "%s: no such table: %s\n", __func__,
               p_foreign_data->table_name);
        rc = -1;
        goto err;
    }

    logmsg(LOGMSG_DEBUG, "%s: Got dbtable\n", __func__);

    // generate final destination genids 
    dst_data_genid = bdb_get_cmp_context(db->handle);
    for (i = 0; i < p_foreign_data->num_index_genids; ++i)
        dst_index_genids[i] = bdb_get_cmp_context(db->handle);
    for (i = 0; i < p_foreign_data->num_blob_genids; ++i)
        dst_blob_genids[i] = bdb_get_cmp_context(db->handle);

    logmsg(LOGMSG_DEBUG, "%s: Got genids\n", __func__);
    
    // make sure all the data checks out and we can do the import
    if (bulk_import_data_validate(&local_data, p_foreign_data, dst_data_genid,
                                  dst_index_genids, dst_blob_genids)) {
        logmsg(LOGMSG_ERROR, "%s: failed validation\n", __func__);
        rc = -1;
        goto err;
    }

    logmsg(LOGMSG_DEBUG, "%s: Validated data\n", __func__);

    bulk_import_generate_filenames(
            &local_data, p_foreign_data, dst_data_genid, dst_index_genids,
            dst_blob_genids, &src_files, &dst_files, &num_files, &bdberr
    );

    for (int fileix=0; fileix<num_files; ++fileix) {
        char *src_file = src_files[fileix];
        char *dst_file = dst_files[fileix];

        offset = snprintf(NULL, 0, "%s/%s", p_foreign_data->data_dir, src_file);
        char * src_path = malloc(offset);
        sprintf(src_path, "%s/%s", p_foreign_data->data_dir, src_file);

        offset = snprintf(NULL, 0, "%s/%s", thedb->basedir, dst_file);
        char * dst_path = malloc(offset);
        sprintf(dst_path, "%s/%s", thedb->basedir, dst_file);

        logmsg(LOGMSG_DEBUG, "Blessing src %s then copying to %s\n", src_path, dst_path); 

        rc = bdb_bless_btree(src_path, dst_path);

        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: Blessing files failed with rc %d\n", __func__, rc);
            goto err;
        }
    }

    logmsg(LOGMSG_DEBUG, "%s: Blessed files\n", __func__);

    wrlock_schema_lk();
    rc = bulkimport_switch_files(db, p_foreign_data, dst_data_genid,
                                     dst_index_genids, dst_blob_genids, &info,
                                     &local_data);
    unlock_schema_lk();

    logmsg(LOGMSG_DEBUG, "%s: Switched files\n", __func__);

err:
    if (num_files != 0) {
        for (int fileix=0; fileix<num_files; ++fileix) {
            free(src_files[fileix]);
            free(dst_files[fileix]);
        }
        free(src_files);
        free(dst_files);
    }

    clear_bulk_import_data(&local_data);

    return rc;
}

static int enable_bulk_import_different_tables_update(void *context, void *value)
{
    int val = *(int *)value;
    if (val) {
        gbl_enable_bulk_import = 1;
        gbl_enable_bulk_import_different_tables = 1;
    } else {
        gbl_enable_bulk_import_different_tables = 0;
    }
    return 0;
}

