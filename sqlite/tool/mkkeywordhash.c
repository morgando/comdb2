/*
** Compile and run this standalone program in order to generate code that
** implements a function that will translate alphabetic identifiers into
** parser token codes.
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

/*
** A header comment placed at the beginning of generated code.
*/
static const char zHdr[] = 
  "/***** This file contains automatically generated code ******\n"
  "**\n"
  "** The code in this file has been automatically generated by\n"
  "**\n"
  "**   sqlite/tool/mkkeywordhash.c\n"
  "**\n"
  "** The code in this file implements a function that determines whether\n"
  "** or not a given identifier is really an SQL keyword.  The same thing\n"
  "** might be implemented more directly using a hand-written hash table.\n"
  "** But by using this automatically generated code, the size of the code\n"
  "** is substantially reduced.  This is important for embedded applications\n"
  "** on platforms with limited memory.\n"
  "*/\n"
;

/*
** All the keywords of the SQL language are stored in a hash
** table composed of instances of the following structure.
*/
typedef struct Keyword Keyword;
struct Keyword {
  char *zName;         /* The keyword name */
  char *zTokenType;    /* Token value for this keyword */
  int mask;            /* Code this keyword if non-zero */
  int id;              /* Unique ID for this record */
  int hash;            /* Hash on the keyword */
  int offset;          /* Offset to start of name string */
  int len;             /* Length of this keyword, not counting final \000 */
  int prefix;          /* Number of characters in prefix */
  int longestSuffix;   /* Longest suffix that is a prefix on another word */
  int iNext;           /* Index in aKeywordTable[] of next with same hash */
  int substrId;        /* Id to another keyword this keyword is embedded in */
  int substrOffset;    /* Offset into substrId for start of this keyword */
  char zOrigName[20];  /* Original keyword name before processing */
};

/*
** Define masks used to determine which keywords are allowed
*/
#ifdef SQLITE_OMIT_ALTERTABLE
#  define ALTER      0
#else
#  define ALTER      0x00000001
#endif
#define ALWAYS       0x00000002
#ifdef SQLITE_OMIT_ANALYZE
#  define ANALYZE    0
#else
#  define ANALYZE    0x00000004
#endif
#ifdef SQLITE_OMIT_ATTACH
#  define ATTACH     0
#else
#  define ATTACH     0x00000008
#endif
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#  define AUTOINCR   0x00000010
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
#ifdef SQLITE_OMIT_AUTOINCREMENT
#  define AUTOINCR   0
#else
#  define AUTOINCR   0x00000010
#endif
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
#ifdef SQLITE_OMIT_CAST
#  define CAST       0
#else
#  define CAST       0x00000020
#endif
#ifdef SQLITE_OMIT_COMPOUND_SELECT
#  define COMPOUND   0
#else
#  define COMPOUND   0x00000040
#endif
#ifdef SQLITE_OMIT_CONFLICT_CLAUSE
#  define CONFLICT   0
#else
#  define CONFLICT   0x00000080
#endif
#ifdef SQLITE_OMIT_EXPLAIN
#  define EXPLAIN    0
#else
#  define EXPLAIN    0x00000100
#endif
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#  define FKEY       0x00000200
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
#ifdef SQLITE_OMIT_FOREIGN_KEY
#  define FKEY       0
#else
#  define FKEY       0x00000200
#endif
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
#ifdef SQLITE_OMIT_PRAGMA
#  define PRAGMA     0
#else
#  define PRAGMA     0x00000400
#endif
#ifdef SQLITE_OMIT_REINDEX
#  define REINDEX    0
#else
#  define REINDEX    0x00000800
#endif
#ifdef SQLITE_OMIT_SUBQUERY
#  define SUBQUERY   0
#else
#  define SUBQUERY   0x00001000
#endif
#ifdef SQLITE_OMIT_TRIGGER
#  define TRIGGER    0
#else
#  define TRIGGER    0x00002000
#endif
#if defined(SQLITE_OMIT_AUTOVACUUM) && \
    (defined(SQLITE_OMIT_VACUUM) || defined(SQLITE_OMIT_ATTACH))
#  define VACUUM     0
#else
#  define VACUUM     0x00004000
#endif
#ifdef SQLITE_OMIT_VIEW
#  define VIEW       0
#else
#  define VIEW       0x00008000
#endif
#ifdef SQLITE_OMIT_VIRTUALTABLE
#  define VTAB       0
#else
#  define VTAB       0x00010000
#endif
#ifdef SQLITE_OMIT_AUTOVACUUM
#  define AUTOVACUUM 0
#else
#  define AUTOVACUUM 0x00020000
#endif
#ifdef SQLITE_OMIT_CTE
#  define CTE        0
#else
#  define CTE        0x00040000
#endif
#ifdef SQLITE_OMIT_UPSERT
#  define UPSERT     0
#else
#  define UPSERT     0x00080000
#endif
#ifdef SQLITE_OMIT_WINDOWFUNC
#  define WINDOWFUNC 0
#else
#  define WINDOWFUNC 0x00100000
#endif

/*
** These are the keywords
*/
static Keyword aKeywordTable[] = {
  { "ABORT",            "TK_ABORT",        CONFLICT|TRIGGER       },
  { "ACTION",           "TK_ACTION",       FKEY                   },
  { "ADD",              "TK_ADD",          ALTER                  },
  { "AFTER",            "TK_AFTER",        TRIGGER                },
  { "ALL",              "TK_ALL",          ALWAYS                 },
  { "ALTER",            "TK_ALTER",        ALTER                  },
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  { "ANALYZE",          "TK_ANALYZE",      ALWAYS                 },
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  { "ANALYZE",          "TK_ANALYZE",      ANALYZE                },
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  { "AND",              "TK_AND",          ALWAYS                 },
  { "AS",               "TK_AS",           ALWAYS                 },
  { "ASC",              "TK_ASC",          ALWAYS                 },
  { "ATTACH",           "TK_ATTACH",       ATTACH                 },
  { "AUTOINCREMENT",    "TK_AUTOINCR",     AUTOINCR               },
  { "BEFORE",           "TK_BEFORE",       TRIGGER                },
  { "BEGIN",            "TK_BEGIN",        ALWAYS                 },
  { "BETWEEN",          "TK_BETWEEN",      ALWAYS                 },
  { "BY",               "TK_BY",           ALWAYS                 },
  { "CASCADE",          "TK_CASCADE",      FKEY                   },
  { "CASE",             "TK_CASE",         ALWAYS                 },
  { "CAST",             "TK_CAST",         CAST                   },
  { "CHECK",            "TK_CHECK",        ALWAYS                 },
  { "COLLATE",          "TK_COLLATE",      ALWAYS                 },
  { "COLUMN",           "TK_COLUMNKW",     ALTER                  },
  { "COMMIT",           "TK_COMMIT",       ALWAYS                 },
  { "CONFLICT",         "TK_CONFLICT",     CONFLICT               },
  { "CONSTRAINT",       "TK_CONSTRAINT",   ALWAYS                 },
  { "CREATE",           "TK_CREATE",       ALWAYS                 },
  { "CROSS",            "TK_JOIN_KW",      ALWAYS                 },
  { "CURRENT",          "TK_CURRENT",      WINDOWFUNC             },
  { "CURRENT_DATE",     "TK_CTIME_KW",     ALWAYS                 },
  { "CURRENT_TIME",     "TK_CTIME_KW",     ALWAYS                 },
  { "CURRENT_TIMESTAMP","TK_CTIME_KW",     ALWAYS                 },
  { "DATABASE",         "TK_DATABASE",     ATTACH                 },
  { "DEFAULT",          "TK_DEFAULT",      ALWAYS                 },
  { "DEFERRED",         "TK_DEFERRED",     ALWAYS                 },
  { "DEFERRABLE",       "TK_DEFERRABLE",   FKEY                   },
  { "DELETE",           "TK_DELETE",       ALWAYS                 },
  { "DESC",             "TK_DESC",         ALWAYS                 },
  { "DETACH",           "TK_DETACH",       ATTACH                 },
  { "DISTINCT",         "TK_DISTINCT",     ALWAYS                 },
  { "DO",               "TK_DO",           UPSERT                 },
  { "DROP",             "TK_DROP",         ALWAYS                 },
  { "END",              "TK_END",          ALWAYS                 },
  { "EACH",             "TK_EACH",         TRIGGER                },
  { "ELSE",             "TK_ELSE",         ALWAYS                 },
  { "ESCAPE",           "TK_ESCAPE",       ALWAYS                 },
  { "EXCEPT",           "TK_EXCEPT",       COMPOUND               },
  { "EXCLUSIVE",        "TK_EXCLUSIVE",    ALWAYS                 },
  { "EXCLUDE",          "TK_EXCLUDE",      WINDOWFUNC             },
  { "EXISTS",           "TK_EXISTS",       ALWAYS                 },
  { "EXPLAIN",          "TK_EXPLAIN",      EXPLAIN                },
  { "FAIL",             "TK_FAIL",         CONFLICT|TRIGGER       },
  { "FILTER",           "TK_FILTER",       WINDOWFUNC             },
  { "FOLLOWING",        "TK_FOLLOWING",    WINDOWFUNC             },
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  { "FOR",              "TK_FOR",          ALWAYS                 },
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  { "FOR",              "TK_FOR",          TRIGGER                },
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  { "FOREIGN",          "TK_FOREIGN",      FKEY                   },
  { "FROM",             "TK_FROM",         ALWAYS                 },
  { "FULL",             "TK_JOIN_KW",      ALWAYS                 },
  { "GLOB",             "TK_LIKE_KW",      ALWAYS                 },
  { "GROUP",            "TK_GROUP",        ALWAYS                 },
  { "GROUPS",           "TK_GROUPS",       WINDOWFUNC             },
  { "HAVING",           "TK_HAVING",       ALWAYS                 },
  { "IF",               "TK_IF",           ALWAYS                 },
  { "IGNORE",           "TK_IGNORE",       CONFLICT|TRIGGER       },
  { "IMMEDIATE",        "TK_IMMEDIATE",    ALWAYS                 },
  { "IN",               "TK_IN",           ALWAYS                 },
  { "INDEX",            "TK_INDEX",        ALWAYS                 },
  { "INDEXED",          "TK_INDEXED",      ALWAYS                 },
  { "INITIALLY",        "TK_INITIALLY",    FKEY                   },
  { "INNER",            "TK_JOIN_KW",      ALWAYS                 },
  { "INSERT",           "TK_INSERT",       ALWAYS                 },
  { "INSTEAD",          "TK_INSTEAD",      TRIGGER                },
  { "INTERSECT",        "TK_INTERSECT",    COMPOUND               },
  { "INTO",             "TK_INTO",         ALWAYS                 },
  { "IS",               "TK_IS",           ALWAYS                 },
  { "ISNULL",           "TK_ISNULL",       ALWAYS                 },
  { "JOIN",             "TK_JOIN",         ALWAYS                 },
  { "KEY",              "TK_KEY",          ALWAYS                 },
  { "LEFT",             "TK_JOIN_KW",      ALWAYS                 },
  { "LIKE",             "TK_LIKE_KW",      ALWAYS                 },
  { "LIMIT",            "TK_LIMIT",        ALWAYS                 },
  { "MATCH",            "TK_MATCH",        ALWAYS                 },
  { "NATURAL",          "TK_JOIN_KW",      ALWAYS                 },
  { "NO",               "TK_NO",           FKEY|WINDOWFUNC        },
  { "NOT",              "TK_NOT",          ALWAYS                 },
  { "NOTHING",          "TK_NOTHING",      UPSERT                 },
  { "NOTNULL",          "TK_NOTNULL",      ALWAYS                 },
  { "NULL",             "TK_NULL",         ALWAYS                 },
  { "OF",               "TK_OF",           ALWAYS                 },
  { "OFFSET",           "TK_OFFSET",       ALWAYS                 },
  { "ON",               "TK_ON",           ALWAYS                 },
  { "OR",               "TK_OR",           ALWAYS                 },
  { "ORDER",            "TK_ORDER",        ALWAYS                 },
  { "OTHERS",           "TK_OTHERS",       WINDOWFUNC             },
  { "OUTER",            "TK_JOIN_KW",      ALWAYS                 },
  { "OVER",             "TK_OVER",         WINDOWFUNC             },
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  { "PARTITION",        "TK_PARTITION",    ALWAYS                 },
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  { "PARTITION",        "TK_PARTITION",    WINDOWFUNC             },
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  { "PLAN",             "TK_PLAN",         EXPLAIN                },
  { "PRAGMA",           "TK_PRAGMA",       PRAGMA                 },
  { "PRECEDING",        "TK_PRECEDING",    WINDOWFUNC             },
  { "PRIMARY",          "TK_PRIMARY",      ALWAYS                 },
  { "QUERY",            "TK_QUERY",        EXPLAIN                },
  { "RAISE",            "TK_RAISE",        TRIGGER                },
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  { "RANGE",            "TK_RANGE",        ALWAYS                 },
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  { "RANGE",            "TK_RANGE",        WINDOWFUNC             },
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  { "RECURSIVE",        "TK_RECURSIVE",    CTE                    },
  { "REFERENCES",       "TK_REFERENCES",   FKEY                   },
  { "REGEXP",           "TK_LIKE_KW",      ALWAYS                 },
  { "REINDEX",          "TK_REINDEX",      REINDEX                },
  { "RELEASE",          "TK_RELEASE",      ALWAYS                 },
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  { "RENAME",           "TK_RENAME",       ALWAYS                 },
#else /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  { "RENAME",           "TK_RENAME",       ALTER                  },
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
  { "REPLACE",          "TK_REPLACE",      CONFLICT               },
  { "RESTRICT",         "TK_RESTRICT",     FKEY                   },
  { "RIGHT",            "TK_JOIN_KW",      ALWAYS                 },
  { "ROLLBACK",         "TK_ROLLBACK",     ALWAYS                 },
  { "ROW",              "TK_ROW",          TRIGGER                },
  { "ROWS",             "TK_ROWS",         ALWAYS                 },
  { "SAVEPOINT",        "TK_SAVEPOINT",    ALWAYS                 },
  { "SELECT",           "TK_SELECT",       ALWAYS                 },
  { "SET",              "TK_SET",          ALWAYS                 },
  { "TABLE",            "TK_TABLE",        ALWAYS                 },
  { "TEMP",             "TK_TEMP",         ALWAYS                 },
  { "TEMPORARY",        "TK_TEMP",         ALWAYS                 },
  { "THEN",             "TK_THEN",         ALWAYS                 },
  { "TIES",             "TK_TIES",         WINDOWFUNC             },
  { "TO",               "TK_TO",           ALWAYS                 },
  { "TRANSACTION",      "TK_TRANSACTION",  ALWAYS                 },
  { "TRIGGER",          "TK_TRIGGER",      TRIGGER                },
  { "UNBOUNDED",        "TK_UNBOUNDED",    WINDOWFUNC             },
  { "UNION",            "TK_UNION",        COMPOUND               },
  { "UNIQUE",           "TK_UNIQUE",       ALWAYS                 },
  { "UPDATE",           "TK_UPDATE",       ALWAYS                 },
  { "USING",            "TK_USING",        ALWAYS                 },
  { "VACUUM",           "TK_VACUUM",       VACUUM                 },
  { "VALUES",           "TK_VALUES",       ALWAYS                 },
  { "VIEW",             "TK_VIEW",         VIEW                   },
  { "VIRTUAL",          "TK_VIRTUAL",      VTAB                   },
  { "WHEN",             "TK_WHEN",         ALWAYS                 },
  { "WHERE",            "TK_WHERE",        ALWAYS                 },
  { "WINDOW",           "TK_WINDOW",       WINDOWFUNC             },
  { "WITH",             "TK_WITH",         CTE                    },
  { "WITHOUT",          "TK_WITHOUT",      ALWAYS                 },

#if defined(SQLITE_BUILDING_FOR_COMDB2)
  { "AGGREGATE",         "TK_AGGREGATE",         ALWAYS           },
  { "ALIAS",             "TK_ALIAS",             ALWAYS           },
  { "ANALYZESQLITE",     "TK_ANALYZESQLITE",     ALWAYS           },
  { "ANALYZEEXPERT",     "TK_ANALYZEEXPERT",     ALWAYS           },
  { "AUTHENTICATION",    "TK_AUTHENTICATION",    ALWAYS           },
  { "BLOBFIELD",         "TK_BLOBFIELD",         ALWAYS           },
  { "BULKIMPORT",        "TK_BULKIMPORT",        ALWAYS           },
  { "COMMITSLEEP",       "TK_COMMITSLEEP",       ALWAYS           },
  { "CONSUMER",          "TK_CONSUMER",          ALWAYS           },
  { "CONVERTSLEEP",      "TK_CONVERTSLEEP",      ALWAYS           },
  { "COUNTER",           "TK_COUNTER",           ALWAYS           },
  { "COVERAGE",          "TK_COVERAGE",          ALWAYS           },
  { "CRLE",              "TK_CRLE",              ALWAYS           },
  { "DATA",              "TK_DATA",              ALWAYS           },
  { "DATABLOB",          "TK_DATABLOB",          ALWAYS           },
  { "DATACOPY",          "TK_DATACOPY",          ALWAYS           },
  { "DBPAD",             "TK_DBPAD",             ALWAYS           },
  { "DDL",               "TK_DDL",               ALWAYS           },
  { "DETERMINISTIC",     "TK_DETERMINISTIC",     ALWAYS           },
  { "DISABLE",           "TK_DISABLE",           ALWAYS           },
  { "DISTRIBUTION",      "TK_DISTRIBUTION",      ALWAYS           },
  { "DRYRUN",            "TK_DRYRUN",            ALWAYS           },
  { "EXCLUSIVE_ANALYZE", "TK_EXCLUSIVE_ANALYZE", ALWAYS           },
  { "EXEC",              "TK_EXEC",              ALWAYS           },
  { "EXECUTE",           "TK_EXECUTE",           ALWAYS           },
  { "ENABLE",            "TK_ENABLE",            ALWAYS           },
  { "FORCE",             "TK_FORCE",             ALWAYS           },
  { "FUNCTION",          "TK_FUNCTION",          ALWAYS           },
  { "GENID48",           "TK_GENID48",           ALWAYS           },
  { "GET",               "TK_GET",               ALWAYS           },
  { "GRANT",             "TK_GRANT",             ALWAYS           },
  { "INCLUDE",           "TK_INCLUDE",           ALWAYS           },
  { "INCREMENT",         "TK_INCREMENT",         ALWAYS           },
  { "IPU",               "TK_IPU",               ALWAYS           },
  { "ISC",               "TK_ISC",               ALWAYS           },
  { "KW",                "TK_KW",                ALWAYS           },
  { "LUA",               "TK_LUA",               ALWAYS           },
  { "LZ4",               "TK_LZ4",               ALWAYS           },
  { "MANUAL",            "TK_MANUAL",            ALWAYS           },
  { "MERGE",             "TK_MERGE",             ALWAYS           },
  { "NEXTSEQUENCE",      "TK_CTIME_KW",          ALWAYS           },
  { "NONE",              "TK_NONE",              ALWAYS           },
  { "OP",                "TK_OP",                ALWAYS           },
  { "OPTION",            "TK_OPTION",            ALWAYS           },
  { "OPTIONS",           "TK_OPTIONS",           ALWAYS           },
  { "ODH",               "TK_ODH",               ALWAYS           },
  { "OFF",               "TK_OFF",               ALWAYS           },
  { "PAGEORDER",         "TK_PAGEORDER",         ALWAYS           },
  { "PARTITIONED",       "TK_PARTITIONED",       ALWAYS           },
  { "PASSWORD",          "TK_PASSWORD",          ALWAYS           },
  { "PAUSE",             "TK_PAUSE",             ALWAYS           },
  { "PERIOD",            "TK_PERIOD",            ALWAYS           },
  { "PENDING",           "TK_PENDING",           ALWAYS           },
  { "PROCEDURE",         "TK_PROCEDURE",         ALWAYS           },
  { "PUT",               "TK_PUT",               ALWAYS           },
  { "READ",              "TK_READ",              ALWAYS           },
  { "READONLY",          "TK_READONLY",          ALWAYS           },
  { "REBUILD",           "TK_REBUILD",           ALWAYS           },
  { "REC",               "TK_REC",               ALWAYS           },
  { "RESERVED",          "TK_RESERVED",          ALWAYS           },
  { "RESUME",            "TK_RESUME",            ALWAYS           },
  { "RETENTION",         "TK_RETENTION",         ALWAYS           },
  { "REVOKE",            "TK_REVOKE",            ALWAYS           },
  { "RLE",               "TK_RLE",               ALWAYS           },
  { "ROWLOCKS",          "TK_ROWLOCKS",          ALWAYS           },
  { "SCALAR",            "TK_SCALAR",            ALWAYS           },
  { "SCHEMACHANGE",      "TK_SCHEMACHANGE",      ALWAYS           },
  { "SELECTV",           "TK_SELECTV",           ALWAYS           },
  { "SEQUENCE",          "TK_SEQUENCE",          ALWAYS           },
  { "SKIPSCAN",          "TK_SKIPSCAN",          ALWAYS           },
  { "START",             "TK_START",             ALWAYS           },
  { "SUMMARIZE",         "TK_SUMMARIZE",         ALWAYS           },
  { "THREADS",           "TK_THREADS",           ALWAYS           },
  { "THRESHOLD",         "TK_THRESHOLD",         ALWAYS           },
  { "TIME",              "TK_TIME",              ALWAYS           },
  { "TRUNCATE",          "TK_TRUNCATE",          ALWAYS           },
  { "TUNABLE",           "TK_TUNABLE",           ALWAYS           },
  { "TYPE",              "TK_TYPE",              ALWAYS           },
  { "USERSCHEMA",        "TK_USERSCHEMA",        ALWAYS           },
  { "VERSION",           "TK_VERSION",           ALWAYS           },
  { "WRITE",             "TK_WRITE",             ALWAYS           },
  { "ZLIB",              "TK_ZLIB",              ALWAYS           },
#endif /* defined(SQLITE_BUILDING_FOR_COMDB2) */
};

/* Number of keywords */
static int nKeyword = (sizeof(aKeywordTable)/sizeof(aKeywordTable[0]));

/* Map all alphabetic characters into lower-case for hashing.  This is
** only valid for alphabetics.  In particular it does not work for '_'
** and so the hash cannot be on a keyword position that might be an '_'.
*/
#define charMap(X)   (0x20|(X))

/*
** Comparision function for two Keyword records
*/
static int keywordCompare1(const void *a, const void *b){
  const Keyword *pA = (Keyword*)a;
  const Keyword *pB = (Keyword*)b;
  int n = pA->len - pB->len;
  if( n==0 ){
    n = strcmp(pA->zName, pB->zName);
  }
  assert( n!=0 );
  return n;
}
static int keywordCompare2(const void *a, const void *b){
  const Keyword *pA = (Keyword*)a;
  const Keyword *pB = (Keyword*)b;
  int n = pB->longestSuffix - pA->longestSuffix;
  if( n==0 ){
    n = strcmp(pA->zName, pB->zName);
  }
  assert( n!=0 );
  return n;
}
static int keywordCompare3(const void *a, const void *b){
  const Keyword *pA = (Keyword*)a;
  const Keyword *pB = (Keyword*)b;
  int n = pA->offset - pB->offset;
  if( n==0 ) n = pB->id - pA->id;
  assert( n!=0 );
  return n;
}

/*
** Return a KeywordTable entry with the given id
*/
static Keyword *findById(int id){
  int i;
  for(i=0; i<nKeyword; i++){
    if( aKeywordTable[i].id==id ) break;
  }
  return &aKeywordTable[i];
}

/*
** This routine does the work.  The generated code is printed on standard
** output.
*/
int main(int argc, char **argv){
  int i, j, k, h;
  int bestSize, bestCount;
  int count;
  int nChar;
  int totalLen = 0;
  int aKWHash[1000];  /* 1000 is much bigger than nKeyword */
  char zKWText[2000];

  /* Remove entries from the list of keywords that have mask==0 */
  for(i=j=0; i<nKeyword; i++){
    if( aKeywordTable[i].mask==0 ) continue;
    if( j<i ){
      aKeywordTable[j] = aKeywordTable[i];
    }
    j++;
  }
  nKeyword = j;

  /* Fill in the lengths of strings and hashes for all entries. */
  for(i=0; i<nKeyword; i++){
    Keyword *p = &aKeywordTable[i];
    p->len = (int)strlen(p->zName);
    assert( p->len<sizeof(p->zOrigName) );
    memcpy(p->zOrigName, p->zName, p->len+1);
    totalLen += p->len;
    p->hash = (charMap(p->zName[0])*4) ^
              (charMap(p->zName[p->len-1])*3) ^ (p->len*1);
    p->id = i+1;
  }

  /* Sort the table from shortest to longest keyword */
  qsort(aKeywordTable, nKeyword, sizeof(aKeywordTable[0]), keywordCompare1);

  /* Look for short keywords embedded in longer keywords */
  for(i=nKeyword-2; i>=0; i--){
    Keyword *p = &aKeywordTable[i];
    for(j=nKeyword-1; j>i && p->substrId==0; j--){
      Keyword *pOther = &aKeywordTable[j];
      if( pOther->substrId ) continue;
      if( pOther->len<=p->len ) continue;
      for(k=0; k<=pOther->len-p->len; k++){
        if( memcmp(p->zName, &pOther->zName[k], p->len)==0 ){
          p->substrId = pOther->id;
          p->substrOffset = k;
          break;
        }
      }
    }
  }

  /* Compute the longestSuffix value for every word */
  for(i=0; i<nKeyword; i++){
    Keyword *p = &aKeywordTable[i];
    if( p->substrId ) continue;
    for(j=0; j<nKeyword; j++){
      Keyword *pOther;
      if( j==i ) continue;
      pOther = &aKeywordTable[j];
      if( pOther->substrId ) continue;
      for(k=p->longestSuffix+1; k<p->len && k<pOther->len; k++){
        if( memcmp(&p->zName[p->len-k], pOther->zName, k)==0 ){
          p->longestSuffix = k;
        }
      }
    }
  }

  /* Sort the table into reverse order by length */
  qsort(aKeywordTable, nKeyword, sizeof(aKeywordTable[0]), keywordCompare2);

  /* Fill in the offset for all entries */
  nChar = 0;
  for(i=0; i<nKeyword; i++){
    Keyword *p = &aKeywordTable[i];
    if( p->offset>0 || p->substrId ) continue;
    p->offset = nChar;
    nChar += p->len;
    for(k=p->len-1; k>=1; k--){
      for(j=i+1; j<nKeyword; j++){
        Keyword *pOther = &aKeywordTable[j];
        if( pOther->offset>0 || pOther->substrId ) continue;
        if( pOther->len<=k ) continue;
        if( memcmp(&p->zName[p->len-k], pOther->zName, k)==0 ){
          p = pOther;
          p->offset = nChar - k;
          nChar = p->offset + p->len;
          p->zName += k;
          p->len -= k;
          p->prefix = k;
          j = i;
          k = p->len;
        }
      }
    }
  }
  for(i=0; i<nKeyword; i++){
    Keyword *p = &aKeywordTable[i];
    if( p->substrId ){
      p->offset = findById(p->substrId)->offset + p->substrOffset;
    }
  }

  /* Sort the table by offset */
  qsort(aKeywordTable, nKeyword, sizeof(aKeywordTable[0]), keywordCompare3);

  /* Figure out how big to make the hash table in order to minimize the
  ** number of collisions */
  bestSize = nKeyword;
  bestCount = nKeyword*nKeyword;
  for(i=nKeyword/2; i<=2*nKeyword; i++){
    for(j=0; j<i; j++) aKWHash[j] = 0;
    for(j=0; j<nKeyword; j++){
      h = aKeywordTable[j].hash % i;
      aKWHash[h] *= 2;
      aKWHash[h]++;
    }
    for(j=count=0; j<i; j++) count += aKWHash[j];
    if( count<bestCount ){
      bestCount = count;
      bestSize = i;
    }
  }

  /* Compute the hash */
  for(i=0; i<bestSize; i++) aKWHash[i] = 0;
  for(i=0; i<nKeyword; i++){
    h = aKeywordTable[i].hash % bestSize;
    aKeywordTable[i].iNext = aKWHash[h];
    aKWHash[h] = i+1;
  }

  /* Begin generating code */
  printf("%s", zHdr);
  printf("/* Hash score: %d */\n", bestCount);
  printf("/* zKWText[] encodes %d bytes of keyword text in %d bytes */\n",
          totalLen + nKeyword, nChar+1 );
  for(i=j=k=0; i<nKeyword; i++){
    Keyword *p = &aKeywordTable[i];
    if( p->substrId ) continue;
    memcpy(&zKWText[k], p->zName, p->len);
    k += p->len;
    if( j+p->len>70 ){
      printf("%*s */\n", 74-j, "");
      j = 0;
    }
    if( j==0 ){
      printf("/*   ");
      j = 8;
    }
    printf("%s", p->zName);
    j += p->len;
  }
  if( j>0 ){
    printf("%*s */\n", 74-j, "");
  }
  printf("static const char zKWText[%d] = {\n", nChar);
  zKWText[nChar] = 0;
  for(i=j=0; i<k; i++){
    if( j==0 ){
      printf("  ");
    }
    if( zKWText[i]==0 ){
      printf("0");
    }else{
      printf("'%c',", zKWText[i]);
    }
    j += 4;
    if( j>68 ){
      printf("\n");
      j = 0;
    }
  }
  if( j>0 ) printf("\n");
  printf("};\n");

  printf("/* aKWHash[i] is the hash value for the i-th keyword */\n");
  printf("static const unsigned char aKWHash[%d] = {\n", bestSize);
  for(i=j=0; i<bestSize; i++){
    if( j==0 ) printf("  ");
    printf(" %3d,", aKWHash[i]);
    j++;
    if( j>12 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s};\n", j==0 ? "" : "\n");    

  printf("/* aKWNext[] forms the hash collision chain.  If aKWHash[i]==0\n");
  printf("** then the i-th keyword has no more hash collisions.  Otherwise,\n");
  printf("** the next keyword with the same hash is aKWHash[i]-1. */\n");
  printf("static const unsigned char aKWNext[%d] = {\n", nKeyword);
  for(i=j=0; i<nKeyword; i++){
    if( j==0 ) printf("  ");
    printf(" %3d,", aKeywordTable[i].iNext);
    j++;
    if( j>12 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s};\n", j==0 ? "" : "\n");    

  printf("/* aKWLen[i] is the length (in bytes) of the i-th keyword */\n");
  printf("static const unsigned char aKWLen[%d] = {\n", nKeyword);
  for(i=j=0; i<nKeyword; i++){
    if( j==0 ) printf("  ");
    printf(" %3d,", aKeywordTable[i].len+aKeywordTable[i].prefix);
    j++;
    if( j>12 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s};\n", j==0 ? "" : "\n");    

  printf("/* aKWOffset[i] is the index into zKWText[] of the start of\n");
  printf("** the text for the i-th keyword. */\n");
  printf("static const unsigned short int aKWOffset[%d] = {\n", nKeyword);
  for(i=j=0; i<nKeyword; i++){
    if( j==0 ) printf("  ");
    printf(" %3d,", aKeywordTable[i].offset);
    j++;
    if( j>12 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s};\n", j==0 ? "" : "\n");

  printf("/* aKWCode[i] is the parser symbol code for the i-th keyword */\n");
  printf("static const unsigned char aKWCode[%d] = {\n", nKeyword);
  for(i=j=0; i<nKeyword; i++){
    char *zToken = aKeywordTable[i].zTokenType;
    if( j==0 ) printf("  ");
    printf("%s,%*s", zToken, (int)(14-strlen(zToken)), "");
    j++;
    if( j>=5 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s};\n", j==0 ? "" : "\n");
  printf("/* Check to see if z[0..n-1] is a keyword. If it is, write the\n");
  printf("** parser symbol code for that keyword into *pType.  Always\n");
  printf("** return the integer n (the length of the token). */\n");
  printf("static int keywordCode(const char *z, int n, int *pType){\n");
  printf("  int i, j;\n");
  printf("  const char *zKW;\n");
  printf("  if( n>=2 ){\n");
  printf("    i = ((charMap(z[0])*4) ^ (charMap(z[n-1])*3) ^ n) %% %d;\n",
          bestSize);
  printf("    for(i=((int)aKWHash[i])-1; i>=0; i=((int)aKWNext[i])-1){\n");
  printf("      if( aKWLen[i]!=n ) continue;\n");
  printf("      j = 0;\n");
  printf("      zKW = &zKWText[aKWOffset[i]];\n");
  printf("#ifdef SQLITE_ASCII\n");
  printf("      while( j<n && (z[j]&~0x20)==zKW[j] ){ j++; }\n");
  printf("#endif\n");
  printf("#ifdef SQLITE_EBCDIC\n");
  printf("      while( j<n && toupper(z[j])==zKW[j] ){ j++; }\n");
  printf("#endif\n");
  printf("      if( j<n ) continue;\n");
  for(i=0; i<nKeyword; i++){
    printf("      testcase( i==%d ); /* %s */\n",
           i, aKeywordTable[i].zOrigName);
  }
  printf("      *pType = aKWCode[i];\n");
  printf("      break;\n");
  printf("    }\n");
  printf("  }\n");
  printf("  return n;\n");
  printf("}\n");
  printf("int sqlite3KeywordCode(const unsigned char *z, int n){\n");
  printf("  int id = TK_ID;\n");
  printf("  keywordCode((char*)z, n, &id);\n");
  printf("  return id;\n");
  printf("}\n");
  printf("#define SQLITE_N_KEYWORD %d\n", nKeyword);
  printf("int sqlite3_keyword_name(int i,const char **pzName,int *pnName){\n");
  printf("  if( i<0 || i>=SQLITE_N_KEYWORD ) return SQLITE_ERROR;\n");
  printf("  *pzName = zKWText + aKWOffset[i];\n");
  printf("  *pnName = aKWLen[i];\n");
  printf("  return SQLITE_OK;\n");
  printf("}\n");
  printf("int sqlite3_keyword_count(void){ return SQLITE_N_KEYWORD; }\n");
  printf("int sqlite3_keyword_check(const char *zName, int nName){\n");
  printf("  return TK_ID!=sqlite3KeywordCode((const u8*)zName, nName);\n");
  printf("}\n");

  return 0;
}
