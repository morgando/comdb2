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

/* Constants */
// static const char bulk_import_done_text[] = "DONE\n";
// static const char bulk_import_done_ack_text[] = "DONEACK\n";
static const char *bulk_import_delims = " \n\r\t";
#define SBUF_DEFAULT_TIMEOUT 10;

/* Tunables */
int gbl_enable_bulk_import = 1;
int gbl_enable_bulk_import_different_tables;
int gbl_bulk_import_client_read_timeout = SBUF_DEFAULT_TIMEOUT;
int gbl_bulk_import_client_write_timeout = SBUF_DEFAULT_TIMEOUT;

#define BULKIMPORT_META_TABLE                                                  \
    X(BULKIMPORT_ODH, "odh")                                                   \
    X(BULKIMPORT_IPU, "ipu")                                                   \
    X(BULKIMPORT_ISC, "isc")                                                   \
    X(BULKIMPORT_DC_ODH, "dc_odh")                                             \
    X(BULKIMPORT_COMPR, "compr")                                               \
    X(BULKIMPORT_COMPR_BLOB, "compr_blob")                                     \
    X(BULKIMPORT_DATA_PGSZ, "data_pgsz")                                       \
    X(BULKIMPORT_INDX_PGSZ, "indx_pgsz")                                       \
    X(BULKIMPORT_BLOB_PGSZ, "blob_pgsz")                                       \
    X(BULKIMPORT_END, "end")                                                   \
    X(BULKIMPORT_UNKNOWN, "")

#define X(a, b) a,
typedef enum { BULKIMPORT_META_TABLE } BULKIMPORT_META_KEY;
#undef X

#define X(a, b) b,
char *bulkimport_meta_key_str[] = {BULKIMPORT_META_TABLE};
#undef X

struct bulk_import_thd_request {
    SBUF2 *sb;
    int *keepsocket;
    int rc;
};

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

static int bulk_import_data_unpack(bulk_import_data_t *p_data, char *line);
void bulk_import_data_print(FILE *p_file,
                                   const ImportData *p_data);

static int bulk_import_data_unpack_from_sbuf(bulk_import_data_t *p_data, SBUF2 *sb)
{
    char line[4096 /*arbitrary csc2 max line len*/];

    /* get line of options */
    if (sbuf2gets(line, sizeof(line), sb) < 0) {
        logmsg(LOGMSG_ERROR, "%s: I/O error reading options\n", __func__);
        return -1;
    }

    return bulk_import_data_unpack(p_data, line);

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
 * Reads in and unpacks bulk import data from a sbuf2.
 * @param p_data    pointer to bulk import data struct to populate
 * @param sb    pointer to sbuf2 to read from
 */
static int bulk_import_data_unpack(bulk_import_data_t *p_data, char *line)
{
    char *lasts = NULL;
    char *tok = NULL;
    char *p_end;
    int num = 0;
    int num2 = 0;


    /* clear the data */
    p_data->table_name[0] = '\0';
    p_data->csc2_crc32 = 0;

    p_data->checksums = -1;

    p_data->odh = -1;
    p_data->compress = -1;
    p_data->compress_blobs = -1;

    p_data->dtastripe = -1;
    p_data->blobstripe = -1;
    p_data->num_index_genids = 0;
    p_data->num_blob_genids = 0;

    p_data->filenames_provided = 0;


    /* parse options */
    tok = strtok_r(line, bulk_import_delims, &lasts);
    while (tok) {
        if (strncmp(tok, "table_name: ", 10) == 0) {
            if (strlen(tok + 10) >= sizeof(p_data->table_name)) {
                logmsg(LOGMSG_ERROR, "%s: tablename too large: %s\n", __func__,
                       tok + 10);
                return -1;
            }
            strncpy0(p_data->table_name, tok + 10, sizeof(p_data->table_name));
        } else if (strncmp(tok, "data_dir:", 8) == 0) {
            if (strlen(tok + 8) >= sizeof(p_data->data_dir)) {
                logmsg(LOGMSG_ERROR, "%s: basedir too large: %s\n", __func__,
                       tok + 8);
                return -1;
            }
            strncpy0(p_data->data_dir, tok + 8, sizeof(p_data->data_dir));
        } else if (strncmp(tok, "csc2_crc32:", 10) == 0) {
            /* parse it */
            p_data->csc2_crc32 = strtoul(tok + 10, &p_end, 16 /*base*/);
            if ((p_end == tok) || (*p_end != '\0')) {
                logmsg(LOGMSG_ERROR, "%s: invalid csc2crc32: %s\n", __func__,
                       tok + 10);
                return -1;
            }
        } else if (strncmp(tok, "checksums:", 10) == 0) {
            /* parse it */
            p_data->checksums = strtol(tok + 10, &p_end, 0 /*base*/);
            if ((p_end == tok) || (*p_end != '\0')) {
                logmsg(LOGMSG_ERROR, "%s: invalid checksums: %s\n", __func__,
                       tok + 10);
                return -1;
            }
        } else if (strncmp(tok, "odh:", 4) == 0) {
            /* parse it */
            p_data->odh = strtol(tok + 4, &p_end, 0 /*base*/);
            if ((p_end == tok) || (*p_end != '\0')) {
                logmsg(LOGMSG_ERROR, "%s: invalid odh: %s\n", __func__,
                       tok + 4);
                return -1;
            }
        } else if (strncmp(tok, "compress:", 9) == 0) {
            /* parse it */
            p_data->compress = strtol(tok + 9, &p_end, 0 /*base*/);
            if ((p_end == tok) || (*p_end != '\0')) {
                logmsg(LOGMSG_ERROR, "%s: invalid compress: %s\n", __func__,
                       tok + 9);
                return -1;
            }
        } else if (strncmp(tok, "compress_blobs:", 14) == 0) {
            /* parse it */
            p_data->compress_blobs = strtol(tok + 14, &p_end, 0 /*base*/);
            if ((p_end == tok) || (*p_end != '\0')) {
                logmsg(LOGMSG_ERROR, "%s: invalid compress blobs: %s\n",
                       __func__, tok + 14);
                return -1;
            }
        } else if (strncmp(tok, "dtastripe:", 10) == 0) {
            /* parse it */
            p_data->dtastripe = strtol(tok + 10, &p_end, 0 /*base*/);
            if ((p_end == tok) || (*p_end != '\0')) {
                logmsg(LOGMSG_ERROR, "%s: invalid dtastripe: %s\n", __func__,
                       tok + 10);
                return -1;
            }
        } else if (strncmp(tok, "blobstripe:", 11) == 0) {
            /* parse it */
            p_data->blobstripe = strtol(tok + 11, &p_end, 0 /*base*/);
            if ((p_end == tok) || (*p_end != '\0')) {
                logmsg(LOGMSG_ERROR, "%s: invalid blobstripe: %s\n", __func__,
                       tok + 11);
                return -1;
            }
        } else if (strncmp(tok, "data_genid:", 10) == 0) {
            /* parse it */
            p_data->data_genid =
                flibc_htonll(strtoull(tok + 10, &p_end, 16 /*base*/));
            if ((p_end == tok) || (*p_end != '\0')) {
                logmsg(LOGMSG_ERROR, "%s: invalid datagenid: %s\n", __func__,
                       tok + 10);
                return -1;
            }
        } else if (strncmp(tok, "index_genids:", 11) == 0) {
            /* if we've seen the max amount of index genids already */
            if (p_data->num_index_genids >= MAXINDEX) {
                logmsg(LOGMSG_ERROR, "%s: too many indexgenids: %s\n", __func__,
                       tok + 11);
                return -1;
            }

            /* parse it */
            p_data->index_genids[p_data->num_index_genids] =
                flibc_htonll(strtoull(tok + 11, &p_end, 16 /*base*/));
            if ((p_end == tok) || (*p_end != '\0')) {
                logmsg(LOGMSG_ERROR, "%s: invalid indexgenid: %s\n", __func__,
                       tok + 11);
                return -1;
            }

            ++p_data->num_index_genids;
        } else if (strncmp(tok, "blob_genids:", 10) == 0) {
            /* if we've seen the max amount of blob genids already */
            if (p_data->num_blob_genids >= MAXBLOBS) {
                logmsg(LOGMSG_ERROR, "%s: too many blbogenids: %s\n", __func__,
                       tok + 10);
                return -1;
            }

            /* parse it */
            p_data->blob_genids[p_data->num_blob_genids] =
                flibc_htonll(strtoull(tok + 10, &p_end, 16 /*base*/));
            if ((p_end == tok) || (*p_end != '\0')) {
                logmsg(LOGMSG_ERROR, "%s: invalid blobgenid: %s\n", __func__,
                       tok + 10);
                return -1;
            }

            ++p_data->num_blob_genids;
        } else if (strncmp(tok, "datafile:", 9) == 0) {
            num = strtol(tok + 9, &p_end, 0 /*base*/);
            if ((p_end == tok) || (*p_end != ':') || num < 0 ||
                num >= p_data->dtastripe) {
            filenameerr:
                logmsg(LOGMSG_ERROR, "%s: invalid filename entry: %s\n",
                       __func__, tok);
                return -1;
            }
            p_end++;

            p_data->data_files[num] = strdup(p_end);

            p_data->filenames_provided = 1;
        } else if (strncmp(tok, "indexfile:", 10) == 0) {
            num = strtol(tok + 10, &p_end, 0 /*base*/);
            if ((p_end == tok) || (*p_end != ':') || num < 0 ||
                num >= p_data->num_index_genids)
                goto filenameerr;

            p_end++;

            p_data->index_files[num] = strdup(p_end);
            if (!p_data->index_files[num])
                goto filenameerr;

            p_data->filenames_provided = 1;
        } else if (strncmp(tok, "blobfile:", 9) == 0) {
            num = strtol(tok + 9, &p_end, 0 /*base*/);
            if ((p_end == tok) || (*p_end != ':') || num < 0 ||
                num >= p_data->num_blob_genids)
                goto filenameerr;

            p_end++;

            num2 = strtol(p_end, &p_end, 0 /*base*/);
            if ((p_end == tok) || (*p_end != ':') || num2 < 0 ||
                num2 >= p_data->dtastripe)
                goto filenameerr;

            p_end++;

            p_data->blob_files[num][num2] = strdup(p_end);
            if (!p_data->blob_files[num][num2])
                goto filenameerr;

            p_data->filenames_provided = 1;
        } else {
            logmsg(LOGMSG_ERROR, "%s: unknown option: %s\n", __func__, tok);
            return -1;
        }
        tok = strtok_r(NULL, bulk_import_delims, &lasts);
    }

    /* we check to see that we got all the filenames */
    if (p_data->filenames_provided) {
        for (num = 0; num < p_data->dtastripe; num++) {
            if (p_data->data_files[num] == NULL) {
                logmsg(LOGMSG_ERROR, "%s: missing data stripe %d\n", __func__,
                       num);
                return -1;
            }
        }

        for (num = 0; num < p_data->num_index_genids; num++) {
            if (p_data->index_files[num] == NULL) {
                logmsg(LOGMSG_ERROR, "%s: missing index num %d\n", __func__,
                       num);
                return -1;
            }
        }

        for (num = 0; num < p_data->num_blob_genids; num++) {
            if (p_data->blobstripe) {
                for (num2 = 0; num2 < p_data->dtastripe; num2++) {
                    if (p_data->blob_files[num][num2] == NULL) {
                        logmsg(LOGMSG_ERROR,
                               "%s: missing blobk num %d stripe %d\n", __func__,
                               num, num2);
                        return -1;
                    }
                }
            } else {
                if (p_data->blob_files[num][0] == NULL) {
                    logmsg(LOGMSG_ERROR, "%s: missing blob num %d\n", __func__,
                           num);
                    return -1;
                }
            }
        }
    }

    /* success */
    return 0;
}

/**
 * Clear the strdups in this structures
 *
 * @param p_data    pointer to place that stores bulk import data,
 */
static void clear_bulk_import_data(ImportData *p_data)
{
    //TODO
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
    int len;

    /* clear data that may not get initialized*/
    p_data->compress = 0;
    p_data->compress_blobs = 0;

    /* find the table we're using */
    if (!(db = get_dbtable_by_name(p_data->table_name))) {
        logmsg(LOGMSG_ERROR, "%s: no such table: %s\n", __func__,
               p_data->table_name);
        return -1;
    }

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

    /* get num indicies/blobs */
    p_data->num_index_genids = db->nix;
    p_data->num_blob_genids = db->numblobs;

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

    if (p_data->num_blob_genids > 0) {
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
    unsigned i;

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

    /* version 1 of bulk import does not need to perform any more checks */
    if (p_foreign_data->bulk_import_version == 1) {
        printf("SKIPPING SCHEMA CHECKS\n");
        return 0;
    }

    /* Bulk import (version 0) from a source which has ODH can't be done
     * reliably because they might have been instant schema changed or have
     * in place updates. The older databases don't provide that information. */
    /*if (p_foreign_data->bulk_import_version == 0 && p_foreign_data->odh) {
        fprintf(
            stderr,
            "%s: Source database is running an older, incompatible version\n",
            __func__);
        return -1;
    }*/

    /* compare csc2's crc32s */
    if (p_local_data->csc2_crc32 != p_foreign_data->csc2_crc32) {
        logmsg(LOGMSG_ERROR, "%s: %s csc2's crc32 differs: %x %x\n", __func__,
               p_local_data->table_name, p_local_data->csc2_crc32,
               p_foreign_data->csc2_crc32);
        return -1;
    }

    /* compare data genids */
    if (p_local_data->data_genid == dst_data_genid) {
        logmsg(LOGMSG_ERROR,
               "%s: %s's main data files already have version: %llx\n",
               __func__, p_local_data->table_name, dst_data_genid);
        return -1;
    }

    /* compare number of indicies and blobs */
    if (p_local_data->num_index_genids != p_foreign_data->num_index_genids ||
        p_local_data->num_blob_genids != p_foreign_data->num_blob_genids) {
        logmsg(LOGMSG_ERROR,
               "%s: number of indicies or blobs differ for table: %s "
               "indicies: %zu %zu blobs: %zu %zu\n",
               __func__, p_local_data->table_name,
               p_local_data->num_index_genids, p_foreign_data->num_index_genids,
               p_local_data->num_blob_genids, p_foreign_data->num_blob_genids);
        return -1;
    }

    /* for each index, compare versions */
    for (i = 0; i < p_local_data->num_index_genids; ++i) {
        if (p_local_data->index_genids[i] == p_dst_index_genids[i]) {
            logmsg(LOGMSG_ERROR,
                   "%s: %s's index: %d files already have version: "
                   "%llx\n",
                   __func__, p_local_data->table_name, i,
                   p_dst_index_genids[i]);
            return -1;
        }
    }

    /* for each blob, compare versions */
    for (i = 0; i < p_local_data->num_blob_genids; ++i) {
        if (p_local_data->blob_genids[i] == p_dst_blob_genids[i]) {
            logmsg(LOGMSG_ERROR,
                   "%s: %s's blob: %d files already have version: "
                   "%llx\n",
                   __func__, p_local_data->table_name, i, p_dst_blob_genids[i]);
            return -1;
        }
    }

    /* compare odh options */
    if (p_local_data->odh != p_foreign_data->odh ||
        p_local_data->compress != p_foreign_data->compress ||
        p_local_data->compress_blobs != p_foreign_data->compress_blobs) {
        logmsg(LOGMSG_ERROR,
               "%s: odh flags differ for table: %s odh: %d %d "
               "compress: %d %d compress blobs: %d %d\n",
               __func__, p_local_data->table_name, p_local_data->odh,
               p_foreign_data->odh, p_local_data->compress,
               p_foreign_data->compress, p_local_data->compress_blobs,
               p_foreign_data->compress_blobs);
        return -1;
    }

    /* success */
    return 0;
}

static void get_schemas(SBUF2 *sb, BulkImportMetaInfo *info)
{
    int i;
    char buf[32];

    /* step 1 */
    sbuf2gets(buf, sizeof(buf), sb);
    info->version = atoi(buf);
    printf("SOURCE TABLE: %s HAS %d VERSIONS\n", info->src_name, info->version);

    /* get csc2 for all the versions */
    for (i = 1; i <= info->version; ++i) {
        sbuf2gets(buf, sizeof(buf), sb);
        int size = atoi(buf);
        info->csc2[i] = malloc(size + 1);
        sbuf2fread(info->csc2[i], size, 1, sb);
        info->csc2[i][size] = '\0';

        /* for debugging */
        // printf("Version %d len: %d\n", i, size);
        // fwrite(info->csc2[i], size, 1, stdout);
        // printf("\n", i);
    }
}

static char *get_strval(char *buf, size_t max)
{
    char *ret = NULL;
    char *bmax = buf + max;
    while (*(++buf) != ':' && *buf && buf < bmax)
        ;
    *buf = '\0';
    ret = ++buf;
    while (*(++buf) != '\n' && *buf && buf < bmax)
        ;
    *buf = '\0';
    return ret;
}

static BULKIMPORT_META_KEY str_to_key(const char *str)
{
    int i;
    for (i = 0; i <= BULKIMPORT_END; ++i) {
        if (strcmp(str, bulkimport_meta_key_str[i]) == 0) {
            return i;
        }
    }
    return BULKIMPORT_UNKNOWN;
}

static int get_meta_info(SBUF2 *sb, BulkImportMetaInfo *info)
{
    get_schemas(sb, info);

    char buf[32];
    char *strkey = buf;
    char *strval = NULL;
    printf("%s: TABLE META-DATA\n", __func__);
    while (1) {
        sbuf2gets(buf, sizeof(buf), sb);
        strval = get_strval(buf, sizeof(buf));
        printf("%s:%s ", strkey, strval);

        switch (str_to_key(strkey)) {
        case BULKIMPORT_ODH:
            sscanf(strval, "%d", &info->odh);
            break;
        case BULKIMPORT_COMPR:
            sscanf(strval, "%d", &info->compr);
            break;
        case BULKIMPORT_COMPR_BLOB:
            sscanf(strval, "%d", &info->compr_blob);
            break;
        case BULKIMPORT_IPU:
            sscanf(strval, "%d", &info->ipu);
            break;
        case BULKIMPORT_ISC:
            sscanf(strval, "%d", &info->isc);
            break;
        case BULKIMPORT_DC_ODH:
            sscanf(strval, "%d", &info->dc_odh);
            break;
        case BULKIMPORT_DATA_PGSZ:
            sscanf(strval, "%d", &info->data_pgsz);
            break;
        case BULKIMPORT_INDX_PGSZ:
            sscanf(strval, "%d", &info->index_pgsz);
            break;
        case BULKIMPORT_BLOB_PGSZ:
            sscanf(strval, "%d", &info->blob_pgsz);
            break;
        case BULKIMPORT_END:
            printf("\n");
            return 0;
        case BULKIMPORT_UNKNOWN:
        default:
            printf("\n");
            printf("Unknown bulkimport key: %s\n", strkey);
            return 1;
        }
    }
}

/**
 * Fills in the filenames in the command line
 * and file ids.
 * @param p__data           pointer to struct containing all the local db's
 *                          table's attributes
 * @param p_foreign_data    pointer to struct containing all the foreign db's
 *                          table's attributes
 * @param outbuf            pointer to the output string where cmnd line is
 *created
 * @param buflen            length of the output string buffer
 * @param bdberr            bdb error, if any
 *
 * @return 0 on success !0 otherwise
 */
static int bulk_import_copy_cmd_add_tmpdir_filenames(
    const bulk_import_data_t *p_data, const bulk_import_data_t *p_foreign_data,
    const unsigned long long dst_data_genid,
    const unsigned long long *p_dst_index_genids,
    const unsigned long long *p_dst_blob_genids, char *outbuf, size_t buflen,
    int *bdberr)
{
    struct dbtable *db = NULL;
    int dtanum;
    int ixnum;
    int len;
    int offset = 0;

    /* find the table we're importing TO */
    if (!(db = get_dbtable_by_name(p_data->table_name))) {
        logmsg(LOGMSG_ERROR, "%s: no such table: %s\n", __func__,
               p_data->table_name);
        return -1;
    }

    /* add -dsttmpdir */
    len = snprintf(outbuf + offset, buflen - offset, " -dsttmpdir %s",
                   bdb_get_tmpdir(thedb->bdb_env));
    offset += len;
    if (len < 0 || offset >= buflen) {
        logmsg(LOGMSG_ERROR,
               "%s: adding -dsttmpdir arg failed or string was too "
               "long for buffer this string len: %d total string len: %d\n",
               __func__, len, offset);
        *bdberr = BDBERR_BUFSMALL;
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
            /* add -file */
            len = snprintf(outbuf + offset, buflen - offset, " -file ");
            offset += len;
            if (len < 0 || offset >= buflen) {
                logmsg(LOGMSG_ERROR,
                       "%s: adding data -file arg failed or string was"
                       " too long for buffer this string len: %d total string "
                       "len: %d\n",
                       __func__, len, offset);
                *bdberr = BDBERR_BUFSMALL;
                return -1;
            }

            /* add src filename */
            if (p_foreign_data->filenames_provided) {
                if (dtanum == 0) {
                    len = snprintf(outbuf + offset, buflen - offset, "%s ",
                                   p_foreign_data->data_files[strnum]);
                } else {
                    len = snprintf(
                        outbuf + offset, buflen - offset, "%s ",
                        p_foreign_data->blob_files[dtanum - 1][strnum]);
                }
            } else {
                len = bdb_form_file_name(db->handle, 1 /*is_data_file*/, dtanum,
                                         strnum, src_version_num,
                                         outbuf + offset, buflen - offset);
            }
            offset += len + 1 /*include the space we're about to add*/;
            if (len < 0 || offset >= buflen) {
                logmsg(LOGMSG_ERROR,
                       "%s: adding src data filename failed or string "
                       "was too long for buffer this string len: %d total "
                       "string len: %d\n",
                       __func__, len, offset);
                *bdberr = BDBERR_BUFSMALL;
                return -1;
            }

            /* add a space. this removes the NUL, it will be re added by the
             * form_file_name_ex() below */
            outbuf[offset - 1] = ' ';

            /* add dst filename */
            len = bdb_form_file_name(db->handle, 1 /*is_data_file*/, dtanum,
                                     strnum, dst_version_num, outbuf + offset,
                                     buflen - offset);
            offset += len;
            if (len < 0 || offset >= buflen) {
                logmsg(LOGMSG_ERROR,
                       "%s: adding dst data filename failed or string "
                       "was too long for buffer this string len: %d total "
                       "string len: %d\n",
                       __func__, len, offset);
                *bdberr = BDBERR_BUFSMALL;
                return -1;
            }
        }
    }

    /* for each index add a -file param */
    for (ixnum = 0; ixnum < p_foreign_data->num_index_genids; ixnum++) {
        /* add -file */
        len = snprintf(outbuf + offset, buflen - offset, " -file ");
        offset += len;
        if (len < 0 || offset >= buflen) {
            logmsg(LOGMSG_ERROR,
                   "%s: adding index -file arg failed or string was "
                   "too long for buffer this string len: %d total string len: "
                   "%d\n",
                   __func__, len, offset);
            *bdberr = BDBERR_BUFSMALL;
            return -1;
        }

        /* add src filename */
        if (p_foreign_data->filenames_provided) {
            len = snprintf(outbuf + offset, buflen - offset, "%s",
                           p_foreign_data->index_files[ixnum]);
        } else {
            len = bdb_form_file_name(db->handle, 0 /*is_data_file*/, ixnum,
                                     0 /*strnum*/,
                                     p_foreign_data->index_genids[ixnum],
                                     outbuf + offset, buflen - offset);
        }
        offset += len + 1 /*include the space we're about to add*/;
        if (len < 0 || offset >= buflen) {
            logmsg(LOGMSG_ERROR,
                   "%s: adding src index filename failed or string "
                   "was too long for buffer this string len: %d total string "
                   "len: %d\n",
                   __func__, len, offset);
            *bdberr = BDBERR_BUFSMALL;
            return -1;
        }

        /* add a space. this removes the NUL, it will be re added by the
         * form_file_name_ex() below */
        outbuf[offset - 1] = ' ';

        /* add dst filename */
        len = bdb_form_file_name(db->handle, 0 /*is_data_file*/, ixnum,
                                 0 /*strnum*/, p_dst_index_genids[ixnum],
                                 outbuf + offset, buflen - offset);
        offset += len;
        if (len < 0 || offset >= buflen) {
            logmsg(LOGMSG_ERROR,
                   "%s: adding dst index filename failed or string "
                   "was too long for buffer this string len: %d total string "
                   "len: %d\n",
                   __func__, len, offset);
            *bdberr = BDBERR_BUFSMALL;
            return -1;
        }
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/**
 * Run the command that copies all the data files around and resets their LSNs
 * and file ids.
 * @param p_foreign_data    pointer to struct containing all the foreign db's
 *                          table's attributes
 * @param p_foreign_dbmach  pointer to the name of foreign db's machine
 * @param dst_data_genid    genid that the final destination data files should
 *                          have
 * @param p_dst_index_genids    pointer to array of genids that the final
 *                              desitnation indicies should have, must have
 *                              p_foreign_data->num_index_genids genids
 * @param p_dst_blob_genids pointer to array of genids that the final
 *                          desitnation blobs should have, must have
 *                          p_foreign_data->num_blob_genids genids
 * @return 0 on success !0 otherwise
 */
static int bulk_import_do_copy_cmd(const bulk_import_data_t *p_data,
                                   const bulk_import_data_t *p_foreign_data,
                                   const char *p_foreign_dbmach,
                                   unsigned long long dst_data_genid,
                                   const unsigned long long *p_dst_index_genids,
                                   const unsigned long long *p_dst_blob_genids)
{
    static const size_t popen_io_str_len = 65536; /* arbitrary, must fit any
                                                   * cmd/rsp, large enough
                                                   * that we don't want to
                                                   * hold it on the stack */
    char *p_popen_io_str = NULL;
    FILE *p_popen_file;
    int popen_status;
    int outrc;
    int bdberr;
    int offset;
    int nsiblings;
    const char *hosts[REPMAX];
    int i;

    /* this string will be used for the cmd itself then used to hold output the
     * cmd returns */
    if (!(p_popen_io_str = malloc(popen_io_str_len))) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed trying to get: %zu\n", __func__,
               popen_io_str_len);
        return -1;
    }

    /* from here on don't return, goto done so p_popen_io_str is free()'d */

    /* build the command line */
    offset = snprintf(p_popen_io_str, popen_io_str_len,
                      "/bb/bin/comdb2_bulk_import_copy -src "
                      "%s -srcdir %s -dstenddir %s "
                      "-dstdbname %s",
                      p_foreign_dbmach, p_foreign_data->data_dir,
                      thedb->basedir, thedb->envname);
    if (offset < 0 || offset >= popen_io_str_len) {
        logmsg(LOGMSG_ERROR,
               "%s: data snprintf failed or string was too long for "
               "buffer string len: %d\n",
               __func__, offset);
        outrc = -1;
        goto done;
    }

    /* retrieve cluster information. */
    nsiblings = net_get_all_nodes(thedb->handle_sibling, hosts);

    /* add replicant's node numbers to cmd */
    for (i = 0; i < nsiblings; ++i) {
        if (gbl_myhostname != hosts[i]) {
            int len = snprintf(p_popen_io_str + offset,
                               popen_io_str_len - offset, " -repname %s", hosts[i]);
            offset += len;
            if (len < 0 || offset >= popen_io_str_len) {
                fprintf(
                    stderr,
                    "%s: replicant snprintf failed or string was too "
                    "long for buffer, this string len: %d total string len: "
                    "%d\n",
                    __func__, len, offset);
                outrc = -1;
                goto done;
            }
        }
    }

    /* let bdb add the -file args since it knows the file name format */
    if (bulk_import_copy_cmd_add_tmpdir_filenames(
            p_data, p_foreign_data, dst_data_genid, p_dst_index_genids,
            p_dst_blob_genids, p_popen_io_str + offset,
            popen_io_str_len - offset, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s: failed to add filenames to cmd\n", __func__);
        outrc = -1;
        goto done;
    }

    printf("%s: running: %s\n", __func__, p_popen_io_str);

    /* kick off a child process to copy the data around (and do fixups like
     * reseting LSNs and creating new file_ids */
    if (!(p_popen_file = popen(p_popen_io_str, "r"))) {
        logmsg(LOGMSG_ERROR, "%s: popen failed: %d %s\n", __func__, errno,
               strerror(errno));
        outrc = -1;
        goto done;
    }

    /* while the child is printing */
    while (fgets(p_popen_io_str, popen_io_str_len, p_popen_file))
        printf("%s", p_popen_io_str);

    /* wait for the child to exit */
    if ((popen_status = pclose(p_popen_file)) < 0) {
        logmsg(LOGMSG_ERROR, "%s: pclose failed: %d %s\n", __func__, errno,
               strerror(errno));
        outrc = -1;
        goto done;
    }
    /* if the child failed */
    else if (popen_status) {
        logmsg(LOGMSG_ERROR, "%s: comdb2_bulk_import_copy failed: %d\n",
               __func__, popen_status);
        outrc = -1;
        goto done;
    }

    /* success */
    outrc = 0;

done:
    free(p_popen_io_str);
    p_popen_io_str = NULL;
    return outrc;
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

    if (p_foreign_data->bulk_import_version == 1) {
        bdb_reset_csc2_version(tran, db->tablename, db->schema_version, 1);
        put_db_odh(db, tran, info->odh);
        put_db_compress(db, tran, info->compr);
        put_db_compress_blobs(db, tran, info->compr_blob);
        put_db_inplace_updates(db, tran, info->ipu);
        put_db_instant_schema_change(db, tran, info->isc);
        put_db_datacopy_odh(db, tran, info->dc_odh);
        for (i = 1; i <= info->version; ++i) {
            put_csc2_file(db->tablename, tran, i, info->csc2[i]);
        }
        bdb_set_pagesize_data(db->handle, tran, info->data_pgsz, &bdberr);
        bdb_set_pagesize_index(db->handle, tran, info->index_pgsz, &bdberr);
        bdb_set_pagesize_blob(db->handle, tran, info->blob_pgsz, &bdberr);
    }

    /* commit new versions */
    if (trans_commit_adaptive(&iq, tran, gbl_myhostname)) {
        logmsg(LOGMSG_ERROR, "%s: failed bulk update commit for table: %s\n",
               __func__, p_foreign_data->table_name);
        goto retry_bulk_update;
    }

    if (p_foreign_data->bulk_import_version == 1) {
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
        clear_bulk_import_data(local_data);
        for (i = 1; i <= info->version; ++i) {
            free(info->csc2[i]);
        }
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
    }

    /* everyone should be running bulk_import_version == 1 now --
     * TODO: remove version 0 code as releasing table lock here before
     * reloading the handle is WRONG!
     */
    bdb_tran_abort(thedb->bdb_env, lock_table_tran, &bdberr);
    lock_table_tran = NULL;

    int rc = bdb_llog_scdone(thedb->bdb_env, fastinit, db->tablename,
                             strlen(db->tablename) + 1, 1, &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to send logical log scdone for table: %s "
               "bdberr: %d\n",
               __func__, p_foreign_data->table_name, bdberr);

        outrc = -1;
        goto backout;
    }

    outrc = 0;

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

int bulk_import_v2(ImportData *p_foreign_data)
{
    unsigned i;
    int offset, rc;
    struct dbtable *db;
    char *command = NULL;
    unsigned long long dst_data_genid;
    unsigned long long dst_index_genids[MAXINDEX];
    unsigned long long dst_blob_genids[MAXBLOBS];
    ImportData local_data = IMPORT_DATA__INIT;
    BulkImportMetaInfo info = {0};

    rc = 0;

    // get local data 
    local_data.table_name = strdup(p_foreign_data->table_name);
    if (bulk_import_data_load(&local_data)) {
        logmsg(LOGMSG_ERROR, "%s: failed getting local data\n", __func__);
        rc = -1;
        goto err;
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

    offset = snprintf(NULL, 0,
                      "/bb/bin/comdb2_bulk_import_reset.tsk %s/%s %s/%s", p_foreign_data->data_dir, p_foreign_data->table_name, thedb->basedir, p_foreign_data->table_name);
    command = malloc(offset);
    sprintf(command, "/bb/bin/comdb2_bulk_import_reset.tsk %s/%s %s/%s", p_foreign_data->data_dir, p_foreign_data->table_name, thedb->basedir, p_foreign_data->table_name);

    rc = system(command);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Blessing files failed with rc %d\n", __func__, rc);
        goto err;
    }

    logmsg(LOGMSG_DEBUG, "%s: Blessed files\n", __func__);

    wrlock_schema_lk();
    rc = bulkimport_switch_files(db, p_foreign_data, dst_data_genid,
                                     dst_index_genids, dst_blob_genids, &info,
                                     &local_data);
    unlock_schema_lk();

    logmsg(LOGMSG_DEBUG, "%s: Switched files\n", __func__);

err:
    
    return rc;
}


static int src_db_bulkimport_ver(SBUF2 *sb, int *version)
{
    char buf[64];
    if (sbuf2printf(sb, "bulkimportforeign_ver\n") < 0 || sbuf2flush(sb) < 0) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to send bulkimportforeignver command\n", __func__);
        sbuf2close(sb);
        return 1;
    }

    if (sbuf2gets(buf, sizeof(buf), sb) < 0) {
        sb_errf(sb, "%s: I/O error reading options\n", __func__);
        sbuf2close(sb);
        return 1;
    }

    if (strcmp(buf, appsock_unknown) == 0) {
        /* source database doesn't know about this appsock cmd
         * so fall back to pick's version */
        *version = 0;
        return 0;
    }

    if (strcmp(buf, appsock_unknown_old) == 0) {
        /* source database doesn't know about this appsock cmd
         * so fall back to pick's version */
        *version = 0;
        return 0;
    }

    int v = atoi(buf);
    if (v >= 1) { /* I only support upto version 1 */
        v = 1;
    } else {
        sb_errf(sb, "%s: unknown bulkimport version: %s\n", __func__, buf);
        sbuf2close(sb);
        return 1;
    }

    *version = v;
    return 0;
}

/**
 * Open up a connection to another foreign comdb2 and tell it to start a foreign
 * bulk import
 * @param p_foreign_dbname  pointer to the name of the db to connect to
 * @param p_foreign_dbmach  pointer to the name of the machine the foreign db is
 *                          on
 * @param p_version         (output arg) what version of bulk-import to do
 * @return pointer to sbuf2 connection to the foreign db if successful, NULL
 * otherwise
 */
static SBUF2 *bulk_import_foreign_open_connection(const char *p_foreign_dbmach,
                                                  const char *p_foreign_dbname,
                                                  int *p_version)
{
    SBUF2 *sb;
    char buf[128];
    char *mastercpu;
    char *dbmastercpu;
    int retries = 0;
    int version;
    int sz;

    /* look up the node number for this machine */
    strncpy0(buf, p_foreign_dbmach, sizeof(buf));

    mastercpu = intern(buf);
retry:
    sb = connect_remote_db("comdb2", p_foreign_dbname, "bulkimport", mastercpu, 0);
    if (!sb) {
        return NULL;
    }

    sbuf2settimeout(sb,
                    gbl_bulk_import_client_read_timeout * 1000,
                    gbl_bulk_import_client_write_timeout * 1000);

    if (src_db_bulkimport_ver(sb, &version)) {
        return NULL;
    }

    /* Query the current master. */
    if (sbuf2printf(sb, "whomasterhost\n") < 0 || sbuf2flush(sb) < 0 ||
        (sz = sbuf2gets(buf, sizeof(buf), sb)) < 0) {
        logmsg(LOGMSG_ERROR, "%s: Failed to send whomaster to foreign db\n",
               __func__);
        sbuf2close(sb);
        return NULL;
    }

    /* Remove trailing '\n' */
    if (buf[sz - 1] == '\n') {
        buf[sz - 1] = 0;
    }

    dbmastercpu = intern(buf);
    if (dbmastercpu && dbmastercpu != mastercpu) {
        if (dbmastercpu <= 0) {
            logmsg(LOGMSG_ERROR, "%s: database '%s' has no master\n", __func__,
                   p_foreign_dbname);
            sbuf2close(sb);
            return NULL;
        } else if (dbmastercpu != mastercpu) {
            sbuf2close(sb);
            if (retries >= 1) {
                logmsg(LOGMSG_ERROR,
                       "%s: database '%s' has no master (retried %d "
                       "times)\n",
                       __func__, p_foreign_dbname, retries);
                return NULL;
            }
            retries++;
            mastercpu = dbmastercpu;
            goto retry;
        }
    }

    if (version == 0) {
        // we're connected to master and are going to do a bulkimport (version
        // 0)
        if (sbuf2printf(sb, "bulkimportforeign\n") < 0 || sbuf2flush(sb) < 0) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to send bulkimportforeign command\n", __func__);
            sbuf2close(sb);
            return NULL;
        }
    } else if (version == 1) {
        if (sbuf2printf(sb, "bulkimportforeign_v1\n") < 0 ||
            sbuf2flush(sb) < 0) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to send bulkimportforeign_v1 command\n",
                   __func__);
            sbuf2close(sb);
            return NULL;
        }
    }

    *p_version = version;
    return sb;
}



static int bulk_import_data_pack(const bulk_import_data_t *p_data, SBUF2 *sb)
{
    unsigned i, j;
    void *p_buf, *p_buf_end;
    unsigned long long genid;

    p_buf = &genid;
    p_buf_end = &genid + sizeof(unsigned long long);
    p_buf = buf_put(&p_data->data_genid, sizeof(unsigned long long), p_buf,
                    p_buf_end);
    if (!p_buf) {
        logmsg(LOGMSG_ERROR, "%s: error converting genid dta\n", __func__);
        return -1;
    }
    /* pack most options */
    if (sbuf2printf(sb,
                    "tablename:%s datadir:%s csc2crc32:%x checksums:%d "
                    "odh:%d compress:%d compressblobs:%d dtastripe:%d "
                    "blobstripe:%d datagenid:%llx",
                    p_data->table_name, p_data->data_dir, p_data->csc2_crc32,
                    p_data->checksums, p_data->odh, p_data->compress,
                    p_data->compress_blobs, p_data->dtastripe,
                    p_data->blobstripe, genid) < 0) {
        logmsg(LOGMSG_ERROR, "%s: error printing options\n", __func__);
        return -1;
    }
    if (gbl_enable_bulk_import_different_tables) {
        /* dumping data stripe file names */
        for (i = 0; i < p_data->dtastripe; i++) {
            sbuf2printf(sb, " datafile:%d:%s", i, p_data->data_files[i]);
        }
    }

    /* pack index genids */
    for (i = 0; i < p_data->num_index_genids; ++i) {
        p_buf = &genid;
        p_buf_end = &genid + sizeof(unsigned long long);
        p_buf = buf_put(&p_data->index_genids[i], sizeof(unsigned long long),
                        p_buf, p_buf_end);
        if (!p_buf) {
            logmsg(LOGMSG_ERROR, "%s: error converting genid ix %d\n", __func__,
                   i);
            return -1;
        }
        if (sbuf2printf(sb, " indexgenid:%llx", genid) < 0) {
            logmsg(LOGMSG_ERROR, "%s: error printing indexgenid\n", __func__);
            return -1;
        }

        if (gbl_enable_bulk_import_different_tables) {
            sbuf2printf(sb, " indexfile:%d:%s", i, p_data->index_files[i]);
        }
    }

    /* pack blob genids */
    for (i = 0; i < p_data->num_blob_genids; ++i) {
        p_buf = &genid;
        p_buf_end = &genid + sizeof(unsigned long long);
        p_buf = buf_put(&p_data->blob_genids[i], sizeof(unsigned long long),
                        p_buf, p_buf_end);
        if (!p_buf) {
            logmsg(LOGMSG_ERROR, "%s: error converting genid blob %d\n",
                   __func__, i);
            return -1;
        }

        if (sbuf2printf(sb, " blobgenid:%llx", genid) < 0) {
            logmsg(LOGMSG_ERROR, "%s: error printing blobgenid\n", __func__);
            return -1;
        }

        if (gbl_enable_bulk_import_different_tables) {
            if (p_data->blobstripe) {
                for (j = 0; j < p_data->dtastripe; j++) {
                    sbuf2printf(sb, " blobfile:%d:%d:%s", i, j,
                                p_data->blob_files[i][j]);
                }
            } else {
                sbuf2printf(sb, " blobfile:%d:%d:%s", i, 0,
                            p_data->blob_files[i][0]);
            }
        }
    }

    /* add newline to mark end of options */
    if (sbuf2printf(sb, "\n") < 0 || sbuf2flush(sb) < 0) {
        logmsg(LOGMSG_ERROR, "%s: error printing or flushing newline\n",
               __func__);
        return -1;
    }

    /* success */
    return 0;
}

static void send_page_sizes(SBUF2 *sb, struct dbtable *db)
{
    int pgsz;
    int bdberr;
    if (bdb_get_pagesize_data(db->handle, NULL, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        sbuf2printf(sb, "%s:%d\n",
                    bulkimport_meta_key_str[BULKIMPORT_DATA_PGSZ], pgsz);
    }
    if (bdb_get_pagesize_index(db->handle, NULL, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        sbuf2printf(sb, "%s:%d\n",
                    bulkimport_meta_key_str[BULKIMPORT_INDX_PGSZ], pgsz);
    }
    if (bdb_get_pagesize_blob(db->handle, NULL, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        sbuf2printf(sb, "%s:%d\n",
                    bulkimport_meta_key_str[BULKIMPORT_BLOB_PGSZ], pgsz);
    }
}

static void send_schema(SBUF2 *sb, struct dbtable *db, int version)
{
    char *text = NULL;
    int len = 0;
    get_csc2_file(db->tablename, version, &text, &len);
    sbuf2printf(sb, "%d\n", len);
    sbuf2flush(sb);
    sbuf2fwrite(text, len, 1, sb);
    sbuf2flush(sb);
    free(text);
}

static void send_all_schemas(SBUF2 *sb, struct dbtable *db)
{
    int i;
    sbuf2printf(sb, "%d\n", db->schema_version);
    for (i = 1; i <= db->schema_version; ++i) {
        send_schema(sb, db, i);
    }
}

static void send_current_schema(SBUF2 *sb, struct dbtable *db)
{
    sbuf2printf(sb, "%d\n", 1);
    send_schema(sb, db, db->schema_version);
}

static void send_meta_info(SBUF2 *sb, struct dbtable *db)
{
    if (db->instant_schema_change) {
        send_all_schemas(sb, db);
    } else {
        send_current_schema(sb, db);
    }

    int value;

    get_db_odh(db, &value);
    sbuf2printf(sb, "%s:%d\n", bulkimport_meta_key_str[BULKIMPORT_ODH], value);

    get_db_inplace_updates(db, &value);
    sbuf2printf(sb, "%s:%d\n", bulkimport_meta_key_str[BULKIMPORT_IPU], value);

    get_db_instant_schema_change(db, &value);
    sbuf2printf(sb, "%s:%d\n", bulkimport_meta_key_str[BULKIMPORT_ISC], value);

    get_db_datacopy_odh(db, &value);
    sbuf2printf(sb, "%s:%d\n", bulkimport_meta_key_str[BULKIMPORT_DC_ODH],
                value);

    get_db_compress(db, &value);
    sbuf2printf(sb, "%s:%d\n", bulkimport_meta_key_str[BULKIMPORT_COMPR],
                value);

    get_db_compress_blobs(db, &value);
    sbuf2printf(sb, "%s:%d\n", bulkimport_meta_key_str[BULKIMPORT_COMPR_BLOB],
                value);

    send_page_sizes(sb, db);

    /* "end" has to be the last one to go down the wire */
    value = 0; /* dummy value */
    sbuf2printf(sb, "%s:%d\n", bulkimport_meta_key_str[BULKIMPORT_END], value);

    sbuf2flush(sb);
}


static int handle_bulk_import_foreign_ver(comdb2_appsock_arg_t *arg)
{
    SBUF2 *sb = arg->sb;
    /*
      Another Comdb2 has asked us to give it the max version of
      bulkimport that I can perform.
    */
    sbuf2printf(sb, "1\n");
    sbuf2flush(sb);
    return APPSOCK_RETURN_CONT;
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

static int bulk_import_timeout_sanitize(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    int val = *(int *)value;
    if (val<=0) {
        // sbuf timeout values can't be 0. that disables the timeouts
        *(int *)tunable->var = SBUF_DEFAULT_TIMEOUT; // set it to the default value;
    } else {
        *(int *)tunable->var = val;
    }
    return 0;
}

/*
  Note: Static plugins are installed before the lrl files are
  parsed, so it should be safe to enable bulk import here.
*/
static int bulk_import_init(void *unused)
{
    REGISTER_TUNABLE("enable_bulk_import", "Enable bulk import (Default: 0)",
                     TUNABLE_BOOLEAN, &gbl_enable_bulk_import, NOARG, NULL,
                     NULL, NULL, NULL);
    REGISTER_TUNABLE("enable_bulk_import_different_tables",
                     "Enable bulk "
                     "import across tables with different names (Default: 0)",
                     TUNABLE_BOOLEAN, &gbl_enable_bulk_import_different_tables,
                     NOARG, NULL, NULL,
                     enable_bulk_import_different_tables_update, NULL);
    REGISTER_TUNABLE("bulk_import_client_read_timeout", "sbuf timeout for reading from foreign DB (Default: 10s)",
                      TUNABLE_INTEGER, &gbl_bulk_import_client_read_timeout,
                      NOARG, NULL, NULL,
                      bulk_import_timeout_sanitize, NULL);
    REGISTER_TUNABLE("bulk_import_client_write_timeout", "sbuf timeout for writing to foreign DB (Default: 10s)",
                      TUNABLE_INTEGER, &gbl_bulk_import_client_write_timeout,
                      NOARG, NULL, NULL,
                      bulk_import_timeout_sanitize, NULL);
    return 0;
}
