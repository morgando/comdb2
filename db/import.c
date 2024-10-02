/**
 * Import allows you to quickly copy a table from one db to another.
 * This is useful when you have to replace a table's
 * contents (i.e. you get a new batch of information every month) but you don't
 * want to slow the main db down by adding all the new records and deleting the
 * old ones in a batch update.
 *
 * This is how it works:
 *
 * 1. Clients issue the SQL statement "import <src_table_name> from <src_db_name> to <dst_table_name>"
 * on the destination db.
 *
 * 2. The destination db starts a comdb2 database in "import mode" that lives in 
 * the destination db's tmp directory (eg: dbdir/tmp/import)
 *
 * 3. The import database grabs all files needed for recovery from the source db
 * by selecting from comdb2_files and then writes these files into its directory.
 *
 * 4. The import database runs full recovery using the files it grabbed from the source.
 *
 * 5. The import database writes information about the import target table 
 * to a file and then terminates.
 *
 * 6. The destination db reads the file written by the import database and uses this 
 * information to copy the btree files associated with the target table from the 
 * import database directory into its directory, copy the files to the replicants,
 * and finally to overwrite the existing table with a schema change.
 */

#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "fdb_fend.h"
#include "bdb_api.h"
#include "bdb_schemachange.h"
#include "comdb2.h"
#include "comdb2_appsock.h"
#include "crc32c.h"
#include "importdata.pb-c.h"
#include "intern_strings.h"
#include "logmsg.h"
#include "sc_callbacks.h"
#include "sc_global.h"
#include "str0.h"

extern void get_txndir_args(char *txndir, size_t sz_txndir, const char *dbdir);
extern char *comdb2_get_tmp_dir();
extern tran_type *curtran_gettran(void);
extern void curtran_puttran(tran_type *tran);
extern int gbl_import_mode;
extern char *gbl_import_table;
extern char *gbl_file_copier;
extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern char *gbl_dbdir;
extern int should_ignore_btree(const char *filename,
                               int (*should_ignore_table)(const char *),
                               int should_ignore_queues,
                               int name_boundary_exists);

/* Constants */
#define FILENAMELEN 100

int bulk_import_tmpdb_should_ignore_table(const char *table) {
    const char *required_tables[] =
        {"comdb2_llmeta", "sqlite_stat1", "sqlite_stat4", "comdb2_metadata", gbl_import_table, "" /* sentinel */};
    const char *required_table;

    for (int i=0; (required_table = required_tables[i]), required_table[0] != '\0'; ++i) {
        if (strcmp(required_table, table) == 0) {
            return 0;
        }
    }

    return 1;
}

int bulk_import_tmpdb_should_ignore_btree(const char *filename)
{
    return should_ignore_btree(filename, bulk_import_tmpdb_should_ignore_table, 0, 1);
}

static void get_import_dbdir(char *import_dbdir, size_t sz_import_dbdir) {
    snprintf(import_dbdir, sz_import_dbdir, "%s/import", comdb2_get_tmp_dir());
}

static int bulk_import_get_import_data_fname(char *import_data_fname,
                                              size_t sz_import_data_fname) {
    int rc;
    char import_dbdir[PATH_MAX];

    rc = 0;

    if (!gbl_import_mode) {
        get_import_dbdir(import_dbdir, sizeof(import_dbdir));
    } else {
        strncpy(import_dbdir, thedb->basedir, sizeof(import_dbdir));
    }

    rc = snprintf(import_data_fname, sz_import_data_fname, "%s/%s", import_dbdir,
             "bulk_import_data") < 0;

    return rc;
}

/*
 * Copies a file to a node using the tool given by the
 * `file_copier` tunable.
 *
 * dst_path: The path of the file to be copied.
 * hostname: The name of the host to copy the file to.
 *
 * returns
 *      0 on success
 *      non-0 on failure
 */
int bulk_import_copy_file_to_replicant(const char *dst_path,
                                       const char *hostname) {
    char *command;
    int offset, rc;

    rc = offset = 0;
    command = NULL;

    offset = snprintf(NULL, 0, "%s -r %s %s:%s", gbl_file_copier, dst_path,
                      hostname, dst_path);
    command = malloc(++offset);
    if (command == NULL) {
        rc = ENOMEM;
        goto err;
    }

    offset = sprintf(command, "%s -r %s %s:%s", gbl_file_copier, dst_path,
                     hostname, dst_path);

    rc = system(command);
    if (rc) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: Failed to copy %s to replicant %s\n",
               __func__, dst_path, hostname);
        goto err;
    }

err:
    if (command != NULL) {
        free(command);
    }
    return rc;
}

/*
 * Cleans up all resources created in `bulk_import_setup_import_db`.
 *
 * p_tmpDbDir: The path of the db directory of the import db to be cleaned.
 *
 * returns
 *      0 on success
 *      non-0 on failure
 */
static int bulk_import_cleanup_import_db(char *tmpDbDir) {
    char *command;
    int size, rc;

    command = NULL;
    rc = size = 0;

    size = snprintf(NULL, 0, "rm -rf %s", tmpDbDir);
    command = malloc(++size);
    if (!command) {
        rc = ENOMEM;
        goto err;
    }

    sprintf(command, "rm -rf %s", tmpDbDir);
    if ((rc = system(command)), rc != 0) {
        goto err;
    }

    free(tmpDbDir);

err:
    if (command) {
        free(command);
    }

    return rc;
}

/*
 * Creates all directories and files needed to run the import db.
 *
 * p_tmpDbDir: Will point to the path of the import db directory on success.
 *
 * returns
 *      0 on success
 *      non-0 on failure
 */
int bulk_import_setup_import_db(char **p_tmpDbDir) {
    char tmpDbDir[PATH_MAX];
    char tmpDbLogDir[PATH_MAX];
    char tmpDbTmpDir[PATH_MAX];
    char fname[PATH_MAX];
    FILE *fp;
    int rc, dbdir_created, logdir_created, tmpdir_created;

    fp = NULL;
    rc = dbdir_created = logdir_created = tmpdir_created = 0;
    get_import_dbdir(tmpDbDir, sizeof(tmpDbDir));

    rc = snprintf(tmpDbLogDir, sizeof(tmpDbLogDir), "%s/logs", tmpDbDir) < 0;
    if (rc != 0) {
        goto err;
    }

    rc = snprintf(tmpDbTmpDir, sizeof(tmpDbTmpDir), "%s/tmp", tmpDbDir) < 0;
    if (rc != 0) {
        goto err;
    }

    rc = snprintf(fname, sizeof(fname), "%s/import.lrl", tmpDbDir) < 0;
    if (rc != 0) {
        goto err;
    }

    rc = mkdir(tmpDbDir, 0700);
    if (rc != 0) {
        // Don't exclude failure when the directory already exists:
        // The import directory is removed after the import; however it may
        // still exist if the remove failed. Running an import on an old
        // directory may result in undefined behavior.
        // TODO: Could probably just warn here because of archival.
        logmsg(LOGMSG_ERROR,
               "%s: Failed to create import dir '%s' with errno %s\n", __func__,
               tmpDbDir, strerror(errno));
        goto err;
    }
    dbdir_created = 1;

    rc = mkdir(tmpDbLogDir, 0700);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "%s: Failed to create import log dir '%s' with errno %s\n",
               __func__, tmpDbLogDir, strerror(errno));
        goto err;
    }
    logdir_created = 1;

    rc = mkdir(tmpDbTmpDir, 0700);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "%s: Failed to create import tmp dir '%s' with errno %s\n",
               __func__, tmpDbTmpDir, strerror(errno));
        goto err;
    }
    tmpdir_created = 1;

    fp = fopen(fname, "w");
    if (fp == NULL) {
        logmsg(LOGMSG_ERROR,
               "%s: Failed to open %s for writing with errno %s\n", __func__,
               fname, strerror(errno));
        rc = 1;
        goto err;
    }

    fprintf(fp, "name import\ndir %s", tmpDbDir);

    rc = fclose(fp);
    if (rc == EOF) {
        goto err;
    }

    *p_tmpDbDir = strdup(tmpDbDir);
    if (*p_tmpDbDir == NULL) {
        rc = ENOMEM;
        goto err;
    }

err:
    if (rc) {
        if (tmpdir_created) {
            if (rmdir(tmpDbTmpDir)) {
                logmsg(LOGMSG_ERROR,
                       "%s: Failed to remove dir '%s' with errno %s\n",
                       __func__, tmpDbTmpDir, strerror(errno));
            }
        }

        if (logdir_created) {
            if (rmdir(tmpDbLogDir)) {
                logmsg(LOGMSG_ERROR,
                       "%s: Failed to remove dir '%s' with errno %s\n",
                       __func__, tmpDbLogDir, strerror(errno));
            }
        }

        if (dbdir_created) {
            if (rmdir(tmpDbDir)) {
                logmsg(LOGMSG_ERROR,
                       "%s: Failed to remove dir '%s' with errno %s\n",
                       __func__, tmpDbDir, strerror(errno));
            }
        }
    }

    return rc;
}

/*
 * Loads import information generated by the import process.
 *
 * pp_data: Will point to a pointer to a populated
 *          `ImportData` struct on success.
 *
 * returns
 *      0 on success
 *      1 on failure
 */
static int bulk_import_data_unpack_from_file(ImportData **pp_data) {
    long fsize;
    int rc;
    void *line;
    FILE *fp;
    char import_data_fname[PATH_MAX];
    struct stat st;

    fsize = 0;
    rc = 0;
    line = NULL;
    fp = NULL;

    bulk_import_get_import_data_fname(import_data_fname,
                                      sizeof(import_data_fname));

    fp = fopen(import_data_fname, "r");
    if (!fp) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: Failed to open file %s. err %s\n",
               __func__, import_data_fname, strerror(errno));
        rc = 1;
        goto err;
    }

    rc = stat(import_data_fname, &st);
    if (rc) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: Failed to stat file %s. err %s\n",
               __func__, import_data_fname, strerror(errno));
        goto err;
    }
    fsize = st.st_size;

    line = malloc(fsize);
    if (!line) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Could not allocate line of size %ld\n", __func__,
               fsize + 1);
        rc = ENOMEM;
        goto err;
    }

    size_t num_read = fread(line, fsize, 1, fp);
    if (num_read < 1) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Read less than expected from file %s. Num read = "
               "%lu and fsize = %lu\n",
               __func__, import_data_fname, num_read, fsize);
        rc = 1;
        goto err;
    }

    *pp_data = import_data__unpack(NULL, fsize, line);
    if (*pp_data == NULL) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: Error unpacking incoming message\n",
               __func__);
        rc = 1;
        goto err;
    }

err:
    if (fp) {
        fclose(fp);
    }

    if (line) {
        free(line);
    }

    return rc;
}

/**
 * Clear the strdups in this structures
 *
 * @param p_data    pointer to place that stores bulk import data,
 */
void clear_bulk_import_data(ImportData *p_data) {
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
    for (int i = 0; i < p_data->n_data_files; ++i) {
        free(p_data->data_files[i]);
    }
    if (p_data->data_files) {
        free(p_data->data_files);
    }
    for (int i = 0; i < p_data->n_index_files; ++i) {
        free(p_data->index_files[i]);
    }
    if (p_data->index_files) {
        free(p_data->index_files);
    }
    for (int i = 0; i < p_data->n_csc2; ++i) {
        free(p_data->csc2[i]);
    }
    if (p_data->csc2) {
        free(p_data->csc2);
    }
    for (int i = 0; i < p_data->n_blob_files; ++i) {
        BlobFiles *b = p_data->blob_files[i];
        for (int j = 0; j < b->n_files; j++) {
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
static int bulk_import_data_load(ImportData *p_data) {
    unsigned i, j;
    int bdberr;
    struct dbtable *db;
    char *p_csc2_text = NULL;
    char tempname[64 /*hah*/];
    tran_type *t = NULL;
    int len, pgsz, rc;

    rc = 0;

    /* clear data that may not get initialized*/
    p_data->compress = 0;
    p_data->compress_blobs = 0;

    /* find the table we're using */
    if (!(db = get_dbtable_by_name(p_data->table_name))) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: no such table: %s\n", __func__,
               p_data->table_name);
        rc = -1;
        goto err;
    }

    /* get the data dir */
    p_data->data_dir = strdup(thedb->basedir);
    if (!p_data->data_dir) {
        rc = ENOMEM;
        goto err;
    }

    /* get table's schema from the meta table and calculate it's crc32 */
    if (get_csc2_file(p_data->table_name, -1 /*highest csc2_version*/,
                      &p_csc2_text, NULL /*csc2len*/)) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: could not get schema for table: %s\n", __func__,
               p_data->table_name);
        rc = -1;
        goto err;
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
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: failed to fetch odh flags for table: %s\n",
               __func__, p_data->table_name);
        rc = -1;
        goto err;
    }

    /* get stripe options */
    p_data->dtastripe = gbl_dtastripe;
    p_data->blobstripe = gbl_blobstripe;

    /* get data file's current version from meta table */
    if (bdb_get_file_version_data(db->handle, NULL /*tran*/, 0 /*dtanum*/,
                                  (unsigned long long *)&p_data->data_genid,
                                  &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: failed to fetch version number for %s's main data "
               "files\n",
               __func__, p_data->table_name);
        rc = -1;
        goto err;
    }

    /* get page sizes from meta table */

    //  * Don't know if this is necessary.
    if (bdb_get_pagesize_data(db->handle, NULL, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        p_data->data_pgsz = pgsz;
    } else {
        p_data->data_pgsz = -1;
    }
    if (bdb_get_pagesize_index(db->handle, NULL, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        p_data->index_pgsz = pgsz;
    } else {
        p_data->index_pgsz = -1;
    }
    if (bdb_get_pagesize_blob(db->handle, NULL, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        p_data->blob_pgsz = pgsz;
    } else {
        p_data->blob_pgsz = -1;
    }

    /* get ipu/isc options from meta table */

    if (get_db_inplace_updates(db, &p_data->ipu)) {
        logmsg(
            LOGMSG_ERROR,
            "[IMPORT] %s: Failed to get inplace update option for table %s\n",
            __func__, p_data->table_name);
    }
    if (get_db_instant_schema_change(db, &p_data->isc)) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Failed to get instant schema change option for "
               "table %s\n",
               __func__, p_data->table_name);
    }
    if (get_db_datacopy_odh(db, &p_data->dc_odh)) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Failed to get datacopy odh option for table %s\n",
               __func__, p_data->table_name);
    }

    p_data->n_data_files = p_data->dtastripe;
    p_data->data_files = malloc(sizeof(char *) * p_data->n_data_files);
    if (!p_data->data_files) {
        rc = ENOMEM;
        goto err;
    }

    for (i = 0; i < p_data->dtastripe; i++) {
        len = bdb_form_file_name(db->handle, 1, 0, i, p_data->data_genid,
                                 tempname, sizeof(tempname));
        if (len <= 0 || len > 64) {
            logmsg(LOGMSG_ERROR,
                   "[IMPORT] %s: failed to retrieve the data filename, stripe "
                   "%d\n",
                   __func__, i);
        }
        p_data->data_files[i] = strdup(tempname);
        if (!p_data->data_files[i]) {
            rc = ENOMEM;
            goto err;
        }
    }

    t = curtran_gettran();
    int version = get_csc2_version_tran(p_data->table_name, t);
    if (version == -1) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Could not find csc2 version for table %s\n",
               __func__, p_data->table_name);
        rc = -1;
        goto err;
    }

    p_data->n_csc2 = version;
    p_data->csc2 = malloc(sizeof(char *) * p_data->n_csc2);
    if (!p_data->csc2) {
        rc = ENOMEM;
        goto err;
    }

    for (int vers = 1; vers <= version; vers++) {
        get_csc2_file_tran(p_data->table_name, vers, &p_data->csc2[vers - 1],
                           &len, t);
    }

    curtran_puttran(t);
    t = NULL;

    /* get num indicies/blobs */
    p_data->num_index_genids = db->nix;
    p_data->n_index_genids = p_data->num_index_genids;
    p_data->index_genids =
        malloc(sizeof(unsigned long int) * p_data->n_index_genids);
    if (!p_data->index_genids) {
        rc = ENOMEM;
        goto err;
    }

    p_data->n_index_files = p_data->n_index_genids;
    p_data->index_files = malloc(sizeof(char *) * p_data->n_index_files);
    if (!p_data->index_files) {
        rc = ENOMEM;
        goto err;
    }

    p_data->num_blob_genids = db->numblobs;
    p_data->n_blob_genids = p_data->num_blob_genids;
    p_data->blob_genids =
        malloc(sizeof(unsigned long int) * p_data->n_blob_genids);
    if (!p_data->blob_genids) {
        rc = ENOMEM;
        goto err;
    }

    /* for each index, lookup version */
    for (i = 0; i < p_data->num_index_genids; ++i) {
        /* get index file's current version from meta table */
        if (bdb_get_file_version_index(
                db->handle, NULL /*tran*/, i /*ixnum*/,
                (unsigned long long *)&p_data->index_genids[i], &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "[IMPORT] %s: failed to fetch version number for %s's "
                   "index: %d files\n",
                   __func__, p_data->table_name, i);
            rc = -1;
            goto err;
        }

        len = bdb_form_file_name(db->handle, 0, 0, i, p_data->index_genids[i],
                                 tempname, sizeof(tempname));
        if (len <= 0 || len > 64) {
            logmsg(
                LOGMSG_ERROR,
                "[IMPORT] %s: failed to retrieve the index filename, ix %d\n",
                __func__, i);
        }
        p_data->index_files[i] = strdup(tempname);
    }

    if (p_data->num_blob_genids > 0) {
        p_data->n_blob_files = p_data->num_blob_genids;
        p_data->blob_files = malloc(sizeof(BlobFiles *) * p_data->n_blob_files);
        if (!p_data->blob_files) {
            rc = ENOMEM;
            goto err;
        }

        for (int i = 0; i < p_data->n_blob_files; ++i) {
            p_data->blob_files[i] = malloc(sizeof(BlobFiles));
            if (!p_data->blob_files[i]) {
                rc = ENOMEM;
                goto err;
            }

            BlobFiles *b = p_data->blob_files[i];
            *b = (BlobFiles) BLOB_FILES__INIT;
            b->n_files = p_data->blobstripe ? p_data->dtastripe : 1;
            b->files = malloc(sizeof(char *) * b->n_files);
            if (!b->files) {
                rc = ENOMEM;
                goto err;
            }
        }
    }

    /* for each blob, lookup and compare versions */
    for (i = 0; i < p_data->num_blob_genids; ++i) {
        /* get blob file's current version from meta table */
        if (bdb_get_file_version_data(
                db->handle, NULL /*tran*/, i + 1 /*dtanum*/,
                (unsigned long long *)&p_data->blob_genids[i], &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "[IMPORT] %s: failed to fetch version number for %s's "
                   "blob: %d files\n",
                   __func__, p_data->table_name, i);
            rc = -1;
            goto err;
        }

        if (p_data->blobstripe) {
            for (j = 0; j < p_data->dtastripe; j++) {
                len = bdb_form_file_name(db->handle, 1, i + 1, j,
                                         p_data->blob_genids[i], tempname,
                                         sizeof(tempname));
                if (len <= 0 || len > 64) {
                    logmsg(LOGMSG_ERROR,
                           "[IMPORT] %s: failed to retrieve the blob "
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
                       "[IMPORT] %s: failed to retrieve the blob filename, "
                       "ix %d stripe %d\n",
                       __func__, i, 0);
            }
            p_data->blob_files[i]->files[0] = strdup(tempname);
        }
    }

err:
    if (t) {
        curtran_puttran(t);
    }

    return rc;
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
                          const ImportData *p_foreign_data) {
    /* lots of sanity checks so that hopefully we never swap in incompatible data
     * files */

    if (thedb->master != gbl_myhostname) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: I'm not the master\n", __func__);
        return -1;
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
               "[IMPORT] %s: stripe settings differ for table: %s dtastripe: "
               "%d %d blobstripe: %d %d\n",
               __func__, p_local_data->table_name, p_local_data->dtastripe,
               p_foreign_data->dtastripe, p_local_data->blobstripe,
               p_foreign_data->blobstripe);
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
 * @param p_foreign_data        pointer to struct containing all the foreign
 * db's table's attributes
 * @param dst_data_genid        data genid on local db.
 * @param p_dst_index_genids    pointer to index genids on local db.
 * @param p_dst_blob_genids     pointer to blob genids on local db.
 * @param p_src_files           pointer to a list of foreign files to be
 * populated.
 * @param p_dst_files           pointer to a list of local files to be
 * populated.
 * @param num_files             pointer to number of files populated in each
 * list.
 * @param bdberr                bdb error, if any
 *
 * @return 0 on success !0 otherwise
 */
static int bulk_import_generate_filenames(
    const ImportData *p_data, const ImportData *p_foreign_data,
    const unsigned long long dst_data_genid,
    const unsigned long long *p_dst_index_genids,
    const unsigned long long *p_dst_blob_genids, char ***p_src_files,
    char ***p_dst_files, int *p_num_files, int *bdberr) {
    struct dbtable *db = NULL;
    int dtanum;
    int ixnum;
    int fileix = 0;

    *p_num_files = p_foreign_data->num_index_genids +
                   (p_foreign_data->blobstripe
                        ? p_foreign_data->num_blob_genids * p_data->dtastripe
                        : p_foreign_data->num_blob_genids) +
                   p_data->dtastripe;
    *p_src_files = malloc(sizeof(char *) * (*p_num_files));
    if (!(*p_src_files)) {
        return ENOMEM;
    }

    *p_dst_files = malloc(sizeof(char *) * (*p_num_files));
    if (!(*p_dst_files)) {
        return ENOMEM;
    }

    for (int i = 0; i < (*p_num_files); ++i) {
        (*p_src_files)[i] = (char *)malloc(FILENAMELEN);
        if (!((*p_src_files)[i])) {
            return ENOMEM;
        }
        
        (*p_dst_files)[i] = (char *)malloc(FILENAMELEN);
        if (!((*p_dst_files)[i])) {
            return ENOMEM;
        }
    }

    char **src_files = *p_src_files;
    char **dst_files = *p_dst_files;

    /* find the table we're importing TO */
    if (!(db = get_dbtable_by_name(p_data->table_name))) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: no such table: %s\n", __func__,
               p_data->table_name);
        return -1;
    }

    /* add data files */
    for (dtanum = 0; dtanum < p_foreign_data->num_blob_genids + 1; dtanum++) {
        int strnum;
        int num_stripes = 0;
        unsigned long long dst_version_num;

        if (dtanum == 0) {
            num_stripes = p_data->dtastripe;
            dst_version_num = dst_data_genid;
        } else {
            if (p_data->blobstripe) {
                num_stripes = p_data->dtastripe;
            } else {
                num_stripes = 1;
            }
            dst_version_num = p_dst_blob_genids[dtanum - 1];
        }

        /* for each stripe add a -file param */
        for (strnum = 0; strnum < num_stripes; ++strnum) {
            /* add src filename */
            if (dtanum == 0) {
                strcpy(src_files[fileix],
                       p_foreign_data->data_files[strnum]);
            } else {
                strcpy(
                    src_files[fileix],
                    p_foreign_data->blob_files[dtanum - 1]->files[strnum]);
            }

            /* add dst filename */
            bdb_form_file_name(db->handle, 1 /*is_data_file*/, dtanum, strnum,
                               dst_version_num, dst_files[fileix], FILENAMELEN);
            fileix++;
        }
    }

    /* for each index add a -file param */
    for (ixnum = 0; ixnum < p_foreign_data->num_index_genids; ixnum++) {
        /* add src filename */
        strcpy(src_files[fileix], p_foreign_data->index_files[ixnum]);

        /* add dst filename */
        bdb_form_file_name(db->handle, 0 /*is_data_file*/, ixnum, 0 /*strnum*/,
                           p_dst_index_genids[ixnum], dst_files[fileix],
                           FILENAMELEN);

        fileix++;
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/*
 * Executes a schema change that makes the import target table point 
 * to the imported btree files.
 *
 * db: dbtable associated with the target table.
 * p_foreign_data: pointer to a struct describing the table being imported.
 * dst_data_genid: genid associated with the new data files.
 * dst_index_genids: genids associated with the new index files.
 * dst_blob_genids: genids associated with the new blob files.
 * local_data: pointer to a struct describing the local target table.
 */
static int bulkimport_switch_files(struct dbtable *db,
                                   const ImportData *p_foreign_data,
                                   unsigned long long dst_data_genid,
                                   unsigned long long *dst_index_genids,
                                   unsigned long long *dst_blob_genids,
                                   ImportData *local_data) {
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
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: failed to close table: %s bdberr: %d\n", __func__,
               p_foreign_data->table_name, bdberr);
        bdb_tran_abort(thedb->bdb_env, lock_table_tran, &bdberr);
        return -1;
    }

    /* from here on use goto backout not return */
    llmeta_dump_mapping_table(thedb, db->tablename, 1);

retry_bulk_update:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: giving up after %d retries\n",
               __func__, retries);

        outrc = -1;
        goto backout;
    }

    if (tran) /* if this is a retry and not the first pass */
    {
        trans_abort(&iq, tran);
        tran = NULL;

        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: bulk update failed for table: %s attempting "
               "retry\n",
               __func__, p_foreign_data->table_name);
    }

    if (trans_start(&iq, NULL /*parent_trans*/, &tran)) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: failed starting bulk update transaction for "
               "table: %s\n",
               __func__, p_foreign_data->table_name);
        goto retry_bulk_update;
    }

    /* update version for main data files */
    if (bdb_new_file_version_data(db->handle, tran, 0 /*dtanum*/,
                                  dst_data_genid, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: failed updating version for table: %s main data "
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
                   "[IMPORT] %s: failed updating version for %s's index: %d "
                   "files new version: %llx\n",
                   __func__, p_foreign_data->table_name, i,
                   (long long unsigned int)p_foreign_data->index_genids[i]);
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
                   "[IMPORT] %s: failed updating version for %s's blob: %d "
                   "files new version: %llx\n",
                   __func__, p_foreign_data->table_name, i,
                   (long long unsigned int)p_foreign_data->blob_genids[i]);
            goto retry_bulk_update;
        }
    }

    if (table_version_upsert(db, tran, &bdberr) || bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: failed to upsert table version bdberr %d\n",
               __func__, bdberr);
        goto retry_bulk_update;
    }

    bdb_reset_csc2_version(tran, db->tablename, db->schema_version, 1);
    put_db_odh(db, tran, p_foreign_data->odh);
    put_db_compress(db, tran, p_foreign_data->compress);
    put_db_compress_blobs(db, tran, p_foreign_data->compress_blobs);
    put_db_inplace_updates(db, tran, p_foreign_data->ipu);
    put_db_instant_schema_change(db, tran, p_foreign_data->isc);
    put_db_datacopy_odh(db, tran, p_foreign_data->dc_odh);
    for (i = 1; i <= p_foreign_data->n_csc2; ++i) {
        put_csc2_file(db->tablename, tran, i, p_foreign_data->csc2[i - 1]);
    }
    if (p_foreign_data->data_pgsz != -1) {
        bdb_set_pagesize_data(db->handle, tran, p_foreign_data->data_pgsz,
                              &bdberr);
    }
    if (p_foreign_data->index_pgsz != -1) {
        bdb_set_pagesize_index(db->handle, tran, p_foreign_data->index_pgsz,
                               &bdberr);
    }
    if (p_foreign_data->blob_pgsz != -1) {
        bdb_set_pagesize_blob(db->handle, tran, p_foreign_data->blob_pgsz,
                              &bdberr);
    }

    /* commit new versions */
    if (trans_commit_adaptive(&iq, tran, gbl_myhostname)) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: failed bulk update commit for table: %s\n",
               __func__, p_foreign_data->table_name);
        goto retry_bulk_update;
    }

    if (reload_after_bulkimport(db, NULL)) {
        /* There is no good way to rollback here. The new schema's were
         * committed but we couldn't reload them (parse error?). Lets just
         * abort here and hope we can do this after bounce */
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: failed reopening table: %s\n",
               __func__, local_data->table_name);
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
               "[IMPORT] %s: failed to send logical log scdone for table: %s "
               "bdberr: %d\n",
               __func__, p_foreign_data->table_name, bdberr);
    }
    return 0;

backout:
    llmeta_dump_mapping_table(thedb, db->tablename, 1 /*err*/);

    /* free the old bdb handle */
    if (bdb_free(db->handle, &bdberr) || bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: failed freeing old db for table: %s bdberr %d\n",
               __func__, p_foreign_data->table_name, bdberr);
        clean_exit();
    }

    /* open the table again, we use bdb_open_more() not bdb_open_again() because
     * the underlying files changed */
    if (!(db->handle = bdb_open_more(
              local_data->table_name, thedb->basedir, 0, db->nix,
              (short *)db->ix_keylen, db->ix_dupes, db->ix_recnums,
              db->ix_datacopy, db->ix_datacopylen, db->ix_collattr,
              db->ix_nullsallowed, db->numblobs + 1 /*main dta*/,
              thedb->bdb_env, &bdberr)) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: failed reopening table: %s, bdberr %d\n", __func__,
               local_data->table_name, bdberr);
        clean_exit();
    }

    bdb_tran_abort(thedb->bdb_env, lock_table_tran, &bdberr);

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DELAYED_OLDFILE_CLEANUP)) {
        /* delete files we don't need now */
        if (bdb_list_unused_files(db->handle, &bdberr, "bulkimport") ||
            bdberr != BDBERR_NOERROR)
            logmsg(
                LOGMSG_ERROR,
                "[IMPORT] %s: errors deleting files for table: %s bdberr: %d\n",
                __func__, local_data->table_name, bdberr);
    } else {
        /* delete files we don't need now */
        if (bdb_del_unused_files(db->handle, &bdberr) ||
            bdberr != BDBERR_NOERROR)
            logmsg(
                LOGMSG_ERROR,
                "[IMPORT] %s: errors deleting files for table: %s bdberr: %d\n",
                __func__, local_data->table_name, bdberr);
    }

    /* if we were successful */
    if (!outrc)
        printf("%s: successful for table: %s\n", __func__,
               local_data->table_name);

    return outrc;
}

/*
 * Completes a bulk import by bringing recovered btree files 
 * from the import process into the db's environment and 
 * executing a schema change so that the target table refers to 
 * these new files.
 *
 * p_foreign_data: Pointer to a struct describing the import data.
 * dst_tablename:  Name of local table into which the data 
 *                 is to be imported.
 *
 * returns
 *  0 on success
 *  non-0 on failure
 */
static int bulk_import_complete(ImportData *p_foreign_data,
                           const char *dst_tablename) {
    unsigned i;
    int offset, num_files, nsiblings, bdberr, loaded_import_data, rc;
    unsigned long long dst_data_genid;
    unsigned long long dst_index_genids[MAXINDEX];
    unsigned long long dst_blob_genids[MAXBLOBS];
    ImportData local_data = IMPORT_DATA__INIT;
    const char *hosts[REPMAX];
    struct dbtable *db;
    char src_path[PATH_MAX];
    char dst_path[PATH_MAX];
    char **src_files, **dst_files;

    rc = num_files = nsiblings = loaded_import_data = bdberr = offset = 0;
    local_data.table_name = strdup(dst_tablename);
    src_files = dst_files = NULL;
    db = NULL;

    rc = bulk_import_data_load(&local_data);
    if (rc) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: failed getting local data\n",
               __func__);
        goto err;
    }
    loaded_import_data = 1;

    db = get_dbtable_by_name(local_data.table_name);
    if (!db) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: no such table: %s\n", __func__,
               p_foreign_data->table_name);
        rc = -1;
        goto err;
    }

    dst_data_genid = bdb_get_cmp_context(db->handle);
    for (i = 0; i < p_foreign_data->num_index_genids; ++i)
        dst_index_genids[i] = bdb_get_cmp_context(db->handle);
    for (i = 0; i < p_foreign_data->num_blob_genids; ++i)
        dst_blob_genids[i] = bdb_get_cmp_context(db->handle);

    // make sure all the data checks out and we can do the import
    rc = bulk_import_data_validate(&local_data, p_foreign_data, dst_data_genid,
                                  dst_index_genids, dst_blob_genids);
    if (rc) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: Failed to validate import with rc %d\n",
                              __func__, rc);
        goto err;
    }

    rc = bulk_import_generate_filenames(&local_data, p_foreign_data, dst_data_genid,
                                   dst_index_genids, dst_blob_genids,
                                   &src_files, &dst_files, &num_files, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: Failed to generate filenames with rc %d\n",
                              __func__, rc);
        goto err;
    }

    for (int fileix = 0; fileix < num_files; ++fileix) {
        snprintf(src_path, sizeof(src_path), "%s/%s", p_foreign_data->data_dir,
                 src_files[fileix]);
        snprintf(dst_path, sizeof(dst_path), "%s/%s", thedb->basedir,
                 dst_files[fileix]);

        rc = bdb_bless_btree(src_path, dst_path);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "[IMPORT] %s: Blessing files failed with rc %d\n",
                   __func__, rc);
            goto err;
        }

        nsiblings = net_get_all_nodes(thedb->handle_sibling, hosts);
        for (int nodeix = 0; nodeix < nsiblings; ++nodeix) {
            if (gbl_myhostname == hosts[nodeix]) { continue; }

            rc = bulk_import_copy_file_to_replicant(dst_path, hosts[nodeix]);
            if (rc) {
                logmsg(LOGMSG_ERROR,
                       "[IMPORT] %s: Failed to copy file to replicant with rc %d\n",
                       __func__, rc);
                goto err;
            }
        }
    }

    wrlock_schema_lk();
    rc =
        bulkimport_switch_files(db, p_foreign_data, dst_data_genid,
                                dst_index_genids, dst_blob_genids, &local_data);
    unlock_schema_lk();

err:
    if (loaded_import_data) {
        clear_bulk_import_data(&local_data);
    }

    if (num_files != 0) {
        for (int fileix = 0; fileix < num_files; ++fileix) {
            free(src_files[fileix]);
            free(dst_files[fileix]);
        }
        free(src_files);
        free(dst_files);
    }

    return rc;
}

static void comdb2_files_tmpdb_get_file_destination(char *copy_dst, ssize_t sz, const char *dir, const char *fname) {
    if (strcmp(dir, "") != 0) {
        snprintf(copy_dst, sz, "%s%s%s%s%s", gbl_dbdir,
             "/", dir, "/", fname);
    } else {
        snprintf(copy_dst, sz, "%s%s%s", gbl_dbdir,
             "/", fname);
        
    }
}

typedef struct str_list_elt {
    char str[PATH_MAX];
    LINKC_T(struct str_list_elt) lnk;
} str_list_elt;
typedef LISTC_T(str_list_elt) str_list;

/*
 * Concatenate C-Strings `dest` and `src`.
 * The result is stored in `dest`.
 * `dest` must be large enough to store the result of the 
 * concatenation.
 *
 * Returns a pointer to the end of the concatenated C-String.
 * This prevents re-scanning on subsequent calls.
 */
char* strcat_and_get_end( char* dest, char* src )
{
     while (*dest) { dest++; }
     while ((*dest++ = *src++)) {}
     return --dest;
}

static char * generate_select_files_query(cdb2_hndl_tp *hndl, str_list *btree_files_to_fetch) {
    char *query_prefix = "select filename, content, dir from comdb2_files where "
                         "type!='berkdb' or (type='berkdb' and filename in (";
    char *query_suffix = ")) order by filename, offset";
    char *query = malloc(strlen(query_prefix) + strlen(query_suffix) + (listc_size(btree_files_to_fetch)*2));
    if (!query) { return NULL; }
    char *p = query;
    *query = '\0';

    p = strcat_and_get_end(p, query_prefix);

    int idx=1;
    str_list_elt *btree_file_elt;
    LISTC_FOR_EACH(btree_files_to_fetch, btree_file_elt, lnk) {
        strcat_and_get_end(p, "?");
        if (btree_file_elt->lnk.next) { strcat_and_get_end(p, ","); }

        cdb2_bind_index(hndl, idx++, CDB2_CSTRING, btree_file_elt->str, strlen(btree_file_elt->str));
    }

    strcat_and_get_end(p, query_suffix);

    return query;
}

static int comdb2_files_tmpdb_process_filenames(cdb2_hndl_tp *hndl, str_list **filename_list_pp) {
    int rc = 0;

    *filename_list_pp
        = listc_new(offsetof(str_list_elt, lnk));
    str_list * const filename_list_p = *filename_list_pp;
    if (!filename_list_p) {
        rc = ENOMEM;
        goto err;
    }

    while (cdb2_next_record(hndl) == CDB2_OK) {
        const char * const fname = (char *)cdb2_column_value(hndl, 0);
        const int fname_len = cdb2_column_size(hndl, 0);

        if (fname_len > PATH_MAX) {
            rc = 1;
            goto err;
        }

        if (!should_ignore_btree(fname, bulk_import_tmpdb_should_ignore_table, 0, 0)) {
            str_list_elt * const elt = calloc(1, sizeof(str_list_elt));
            if (!elt) {
                rc = ENOMEM;
                goto err;
            }

            strncpy(elt->str, fname, sizeof(elt->str));
            listc_atl(filename_list_p, elt);
        }
    }

err:
    if (rc && filename_list_p) {
        LISTC_CLEAN(filename_list_p, lnk, 1, str_list_elt);
        free(filename_list_p);
    }

    return rc;
}

static int comdb2_files_tmpdb_process_incoming_files(cdb2_hndl_tp *hndl) {
    char * fname;
    int rc, fd;

    fname = NULL;
    rc = 0;
    fd = -1;

    while (cdb2_next_record(hndl) == CDB2_OK) {
        const char *next_fname = (char *)cdb2_column_value(hndl, 0);
        const char *chunk_content = (char *)cdb2_column_value(hndl, 1);
        const char *dir = (char *)cdb2_column_value(hndl, 2);
        const int chunk_size = cdb2_column_size(hndl, 1);
        char copy_dst[PATH_MAX];

        if (strcmp(next_fname, "checkpoint") == 0) {
            continue;
        }
        if (fname == NULL || strcmp(fname, next_fname) != 0) {
            // We haven't processed this file yet

            if (fname != NULL) {
                free(fname);
            }
            if (fd != -1) {
                Close(fd);
            }

            fname = strdup(next_fname);

            comdb2_files_tmpdb_get_file_destination(copy_dst, sizeof(copy_dst), dir, fname);

            fd = open(copy_dst, O_WRONLY | O_CREAT | O_APPEND, 0755);
            if (fd == -1) {
                logmsg(LOGMSG_ERROR,
                       "[IMPORT] %s: Failed to open file %s (errno: %s)\n",
                       __func__, copy_dst, strerror(errno));
                rc = 1;
                goto err;
            }
        }

        const ssize_t bytes_written = write(fd, chunk_content, chunk_size);
        if (bytes_written != chunk_size) {
            logmsg(LOGMSG_ERROR,
                   "[IMPORT] %s: failed to write to the file (expected: %d "
                   "got: %ld)\n",
                   __func__, chunk_size, bytes_written);
            rc = 1;
            goto err;
        }
    }

err:
    if (fd != -1) {
        Close(fd);
    }

    if (fname != NULL) {
        free(fname);
    }

    return rc;
}

/*
 * Gets foreign db's files and writes them into the local environment.
 *
 * returns
 *  0 on success
 *  non-0 on failure
 */
int bulk_import_tmpdb_pull_foreign_dbfiles(const char *fdb_name) {
    cdb2_hndl_tp *hndl;
    fdb_t *fdb;
    int rc, t_rc;
    char txndir[PATH_MAX];
    char query[2000];

    rc = t_rc = 0;
    hndl = NULL;
    get_txndir_args(txndir, sizeof(txndir), gbl_dbdir);

    rc = create_fdb(fdb_name, &fdb);
    if (rc) {
        logmsg(
            LOGMSG_ERROR,
            "[IMPORT] %s: Failed to create fdb with rc %d\n",
            __func__, rc);
        goto err;
    }

    rc = cdb2_open(&hndl, fdb_dbname_name(fdb), fdb_dbname_class_routing(fdb), 0);
    if (rc) {
        logmsg(
            LOGMSG_ERROR,
            "[IMPORT] %s: Failed to open a handle to src db with rc %d\n",
            __func__, rc);
        goto err;
    }

    snprintf(query, sizeof(query), "exec procedure sys.cmd.send('flush')");
    rc = cdb2_run_statement(hndl, query);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Got an error flushing src db. errstr: %s\n",
               __func__, cdb2_errstr(hndl));
        goto err;
    }
    while (cdb2_next_record(hndl) == CDB2_OK) {}

    snprintf(query, sizeof(query), "select distinct filename from comdb2_files where type='berkdb'");
    rc = cdb2_run_statement(hndl, query);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Failed to get file names from src db. errstr: %s\n",
               __func__, cdb2_errstr(hndl));
        goto err;
    }

    str_list *filename_list;
    rc = comdb2_files_tmpdb_process_filenames(hndl, &filename_list);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Failed to process filenames from src db. rc: %d\n",
               __func__, rc);
        goto err;
    }

    char *select_files_query = generate_select_files_query(hndl, filename_list);
    if (!select_files_query) {
        rc = ENOMEM;
        goto err;
    }

    rc = cdb2_run_statement(hndl, select_files_query);
    free(select_files_query);
    LISTC_CLEAN(filename_list, lnk, 1, str_list_elt);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Got an error grabbing files from src db. errstr: "
               "%s\n",
               __func__, cdb2_errstr(hndl));
        goto err;
    }

    rc = comdb2_files_tmpdb_process_incoming_files(hndl);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Failed to process files from src db. rc: %d",
               __func__, rc);
        goto err;
    }

err:
    if (hndl) {
        t_rc = cdb2_close(hndl);
        if (t_rc) {
            logmsg(LOGMSG_ERROR,
                   "[IMPORT] %s: Failed to close hndl. rc: %d",
                   __func__, t_rc);
        }
    }

    rc = rc != 0 ? rc : t_rc;
    return rc;
}

static int get_my_comdb2_executable(char **p_exe)
{
    int rc, size;
    pid_t pid;

    rc = size = 0;
    pid = getpid();

#if defined(_LINUX_SOURCE)
    size = snprintf(NULL, 0, "/proc/%ld/exe", (long) pid);
    *p_exe = malloc(++size);
    if (*p_exe == NULL) {
        rc = ENOMEM;
        goto err;
    }
    sprintf(*p_exe, "/proc/%ld/exe", (long) pid);
#elif defined(_SUN_SOURCE)
    size = snprintf(NULL, 0, "/proc/%ld/execname", (long) pid);
    *p_exe = malloc(++size);
    if (*p_exe == NULL) {
        rc = ENOMEM;
        goto err;
    }
    sprintf(*p_exe, "/proc/%ld/execname", (long) pid);
#else
    #error "Unsupported platform"
#endif

err:
    return rc;
}


int bulk_import_do_import(const char *srcdb, const char *src_tablename, const char *dst_tablename)
{
    int rc, t_rc, size;
    char *tmpDbDir, *command, *exe;
    char fpath[PATH_MAX];
    ImportData *import_data;
    const char *my_tier = get_my_mach_class_str();

    rc = t_rc = size = 0;
    tmpDbDir = command = exe = NULL;
    import_data = NULL;

    pthread_mutex_lock(&(thedb->import_lock)); // Force imports to run serially, for now.

    rc = bulk_import_setup_import_db(&tmpDbDir);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Failed to setup import db\n", __func__);
        goto err;
    }

    rc = get_my_comdb2_executable(&exe);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: Failed to get my comdb2 executable\n", __func__);
        goto err;
    }

    size = 1 + snprintf(NULL, 0, "%s --import --dir %s --tables %s --src %s --my-tier %s",
                    exe, tmpDbDir, src_tablename, srcdb, my_tier);
    command = malloc(size);
    if (command == NULL) {
        rc = ENOMEM;
        goto err;
    }

    sprintf(command, "%s --import --dir %s --tables %s --src %s --my-tier %s",
            exe, tmpDbDir, src_tablename, srcdb, my_tier);

    rc = system(command);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: Import process failed with rc %d.\n",
               __func__, rc);
        goto err;
    }
    
    rc = bulk_import_data_unpack_from_file(&import_data);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: Failed to unpack import data from %s\n",
               __func__, fpath);
        goto err;
    }

    rc = bulk_import_complete(import_data, dst_tablename);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: Failed to complete import.\n",
               __func__);
        goto err;
    }

err:
    if (command) {
        free(command);
    }

    if (exe) {
        free(exe);
    }

    if (import_data) {
        import_data__free_unpacked(import_data, NULL);
    }

    if (tmpDbDir) {
        t_rc = bulk_import_cleanup_import_db(tmpDbDir);
        if (t_rc != 0) {
            // Don't error here: We've finished the import successfully.
            // Emit a warning because subsequent imports will fail if the import dir already exists
            logmsg(LOGMSG_WARN, "Cleaning up import db failed with rc %d\n", t_rc);
        }
    }
    
    pthread_mutex_unlock(&(thedb->import_lock));
   
    return rc;
}

/*
 * Packs bulk import data about a table to a file.
 *
 * import_table: The name of the table
 *
 * returns
 *      0 on success
 *      non-0 on failure
 */
int bulk_import_tmpdb_write_import_data(const char *import_table) {
    int rc;
    int written_bytes;
    unsigned len;
    void *buf;
    FILE *f_bulk_import;
    char import_file_path[PATH_MAX];
    ImportData import_data = IMPORT_DATA__INIT;

    rc = 0;
    len = written_bytes = 0;
    buf = NULL;
    f_bulk_import = NULL;

    import_data.table_name = strdup(import_table);
    if (import_data.table_name == NULL) {
        rc = ENOMEM;
        goto err;
    }

    rc = bulk_import_data_load(&import_data);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "[IMPORT] %s: Failed to load import data\n", __func__);
        goto err;
    }

    rc = bulk_import_get_import_data_fname(import_file_path, sizeof(import_file_path));
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: Failed to get import data fname\n",
               __func__);
        goto err;
    }

    f_bulk_import = fopen(import_file_path, "w");
    if (f_bulk_import == NULL) {
        logmsg(LOGMSG_ERROR, "[IMPORT] %s: Failed to open file %s. err %s\n",
               __func__, import_file_path, strerror(errno));
        rc = 1;
        goto err;
    }

    len = import_data__get_packed_size(&import_data);
    buf = malloc(len);
    if (buf == NULL) {
        rc = ENOMEM;
        goto err;
    }

    written_bytes = import_data__pack(&import_data, buf);
    if (written_bytes != len) {
        logmsg(LOGMSG_ERROR,
               "[IMPORT] %s: Did not pack full protobuf buffer.\n",
               __func__);
        rc = 1;
        goto err;
    }

    written_bytes = fwrite(buf, 1, len, f_bulk_import);
    if (written_bytes != len) {
        logmsg(
            LOGMSG_ERROR,
            "[IMPORT] %s: Did not write full protobuf buffer to file",
            __func__);
        rc = 1;
        goto err;
    }

err:
    clear_bulk_import_data(&import_data);

    if (f_bulk_import != NULL) {
        fclose(f_bulk_import);
    }

    if (buf != NULL) {
        free(buf);
    }

    return rc;
}
