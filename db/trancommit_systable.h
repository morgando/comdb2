// TODO copyright

#ifndef __TRANCOMMIT_SYSTABLE_H_
#define __TRANCOMMIT_SYSTABLE_H_

typedef struct txn_commit_info {
	u_int64_t utxnid;
	u_int64_t commit_lsn_file;
	u_int64_t commit_lsn_offset;
} txn_commit_info;

#endif
