/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the files COPYING and Copyright.html.  COPYING can be found at the root   *
 * of the source code distribution tree; Copyright.html can be found at the  *
 * root level of an installed copy of the electronic HDF5 document set and   *
 * is linked from the top-level documents page.  It can also be found at     *
 * http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have          *
 * access to either file, you may request a copy from help@hdfgroup.org.     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Warning: Currently in ALPHA, do not use on production. LOG FORMAT WILL CHANGE.
 *
 * Programmer:  Benoit Gschwind <benoit.gschwind@mines-paristech.fr>
 *              based on sec2 driver (2014) by Robb Matzke <matzke@llnl.gov>
 *
 * Purpose: The Safe and Inefficient Driver intend to implement journalized
 *          file write to ensure data integrity. The driver do not write data in
 *          file immediately but it write it into alternate file journal file all
 *          update operation. When the file is close those operation are written on
 *          real file then deleted. If the software that open HDF5 file crash at
 *          any point of this process the file is restored to the previous state
 *          (before we opened it) or to the current valid state (at the moment
 *          of closing the file). By this way, HDF5 cannot be corrupted. The draw
 *          back is temporary disk space consuming and twice slower write.
 *
 * GLOSAIRE :
 *
 *  journal file: the file used for storing operations
 *
 *  original file: the file that we open at open state
 *
 *  final file: the file at the close state
 *
 */

/* Interface initialization */
#define H5_INTERFACE_INIT_FUNC  H5FD_safe_init_interface


#include "H5private.h"      /* Generic Functions        */
#include "H5Eprivate.h"     /* Error handling           */
#include "H5Fprivate.h"     /* File access              */
#include "H5FDprivate.h"    /* File drivers             */
#include "H5FDsafe.h"        /* sid file driver         */
#include "H5FLprivate.h"    /* Free Lists               */
#include "H5Iprivate.h"     /* IDs                      */
#include "H5MMprivate.h"    /* Memory management        */
#include "H5Pprivate.h"     /* Property lists           */


typedef struct H5FD_safe_log_header_t {
	uint64_t magic_id; /* id used for checksum, header validation nad message validation */
	uint64_t old_magic_id; /* if both header are valid, keep the previous number to validate the newer */
	uint64_t file_map_offset; /* offset of valid filemap at start of log */
	uint64_t file_map_fragment_count;
	uint64_t check_sum; /* check sum of header */
} H5FD_safe_log_header_t;

typedef struct H5FD_safe_file_header_t {
	uint64_t signature; /* some thing like hdf5 gnignature */
	uint64_t version;
	uint64_t file_id; /** could be usefull for some checksum */
	struct H5FD_safe_log_header_t log0;
	struct H5FD_safe_log_header_t log1;
} H5FD_safe_file_header_t;

enum file_map_type_e {
	H5FD_SAFE_PROTECTED_DATA,
	H5FD_SAFE_DATA,
	H5FD_SAFE_LOG,
	H5FD_SAFE_FILE_MAP,
	H5FD_SAFE_FREE
};

/** sorted list by offset **/
typedef struct H5FD_safe_alloc_list_t {
	struct H5FD_safe_alloc_list_t * next;
	int64_t offset;
	int64_t size;
}H5FD_safe_alloc_list_t;

static void
H5FD_safe_alloc_list_print(H5FD_safe_alloc_list_t * ths, char const * name) {
	H5FD_safe_alloc_list_t * cur;

	cur = ths;

	printf("%s: ", name);
	while (cur != NULL) {
		printf("[%ld, %ld] -> ", cur->offset, cur->size);
		cur = cur->next;
	}
	printf("NULL\n");

}


static H5FD_safe_alloc_list_t *
H5FD_safe_alloc_list_cleanup(H5FD_safe_alloc_list_t * ths) {
	//printf("H5FD_safe_alloc_list_cleanup(%p)\n", ths);
	//H5FD_safe_alloc_list_print(ths, "anonymous");
	H5FD_safe_alloc_list_t * i = ths;
	while(i != NULL) {
		H5FD_safe_alloc_list_t * j = i->next;
		while(j != NULL) {
			if(i->offset + i->size >= j->offset) {

				printf("merge [%ld,%ld] with [%ld,%ld]\n", i->offset, i->offset + i->size, j->offset, j->offset + j->size);

				i->size = j->offset + j->size - i->offset;
				i->next = j->next;
				printf("INTO: [%ld,%ld]\n", i->offset, i->offset + i->size);
				free(j);
				j = i->next;
			} else {
				j = j->next;
			}
		}
		i = i->next;
	}
	return ths;
}

static H5FD_safe_alloc_list_t *
H5FD_safe_alloc_list_new_node(int64_t offset, int64_t size) {
	H5FD_safe_alloc_list_t * res;
	printf("H5FD_safe_alloc_list_new_node(%ld, %ld)\n", offset, size);
	res = (H5FD_safe_alloc_list_t *)malloc(sizeof(H5FD_safe_alloc_list_t));
	res->offset = offset;
	res->size = size;
	res->next = NULL;
	return res;
}

static H5FD_safe_alloc_list_t *
H5FD_safe_alloc_list_add(H5FD_safe_alloc_list_t * ths, H5FD_safe_alloc_list_t * node) {
	H5FD_safe_alloc_list_t ** cur;
	//printf("H5FD_safe_alloc_list_add(node = [%ld, %ld])\n", node->offset, node->size);
	//H5FD_safe_alloc_list_print(ths, "Before anonymous");

	/* find node position */
	cur = &ths;
	while ((*cur) != NULL) {
		if ((*cur)->offset >= node->offset) {
			break;
		}
		cur = &((*cur)->next);
	}

	/* insert node */
	node->next = *cur;
	*cur = node;

	//H5FD_safe_alloc_list_print(ths, "After anonymous");

	return H5FD_safe_alloc_list_cleanup(ths);
}


/** remove BUT NOT FREE node **/
static H5FD_safe_alloc_list_t *
H5FD_safe_alloc_list_remove(H5FD_safe_alloc_list_t * ths, H5FD_safe_alloc_list_t * node) {
	H5FD_safe_alloc_list_t ** cur;

	cur = &ths;

	while ((*cur) != NULL && (*cur) != node) {
		cur = &((*cur)->next);
	}

	if(*cur != NULL) {
		*cur = (*cur)->next;
	} else {
		printf("WARNING: NODE NOT FOUND!\n");
	}

	return ths;
}

static H5FD_safe_alloc_list_t *
H5FD_safe_negative_mask(H5FD_safe_alloc_list_t * ths) {
	H5FD_safe_alloc_list_t * res = NULL;
	H5FD_safe_alloc_list_t * cur;
	int64_t last_end = 0;

	cur = ths;
	while(cur != NULL) {
		if(cur->offset > last_end) {
			H5FD_safe_alloc_list_t * x = H5FD_safe_alloc_list_new_node(last_end, cur->offset - last_end);
			res = H5FD_safe_alloc_list_add(res, x);
		}
		last_end = cur->offset + cur->size;
		cur = cur->next;
	}

	H5FD_safe_alloc_list_t * x = H5FD_safe_alloc_list_new_node(last_end, INT64_MAX - last_end);
	res = H5FD_safe_alloc_list_add(res, x);
	res = H5FD_safe_alloc_list_cleanup(res);

	return res;
}



typedef struct H5FD_safe_log_handler_t {
	int64_t current_file_offset;
	int64_t free_log_size;
} H5FD_safe_log_handler_t;


/* enum to use in virtual file map to specify the location of data */
typedef enum H5FD_safe_source_id_t {
	H5FD_SAFE_SRC_EMPTY, /* data are not alocated */
	H5FD_SAFE_SRC_FILE,  /* data are located in original file */
	H5FD_SAFE_SRC_LOG    /* data are located in journal file */
} H5FD_safe_source_id_t;


typedef struct H5FD_safe_vfm_serialize_header_t {
	uint64_t node_count;
	uint64_t file_id;
	uint64_t log_id;
	uint64_t hdr_check_sum;
} H5FD_safe_vfm_serialize_header_t;

typedef struct H5FD_safe_vfm_serialize_node_t {
	int64_t voffset;
	int64_t roffset;
	int64_t size;
} H5FD_safe_vfm_serialize_node_t;


/**
 * simple sorted list of file virtual file map.
 *
 * This map allow to create a virtual continuous file that is stored several files. in this case
 * in journal file and/or original file and/or empty area (area that is not already written).
 *
 * By usage this map cannot have node with size == 0.
 *
 */
typedef struct H5FD_safe_vfm_t {
	int64_t voffset;			/* offset in virtual file */
	int64_t roffset;			/* offset in the current real file */
	int64_t size;				/* size of data */
	struct H5FD_safe_vfm_t * next;
	struct H5FD_safe_vfm_t * prev;
} H5FD_safe_virtual_file_map_t;

/* short name for the virtual_file_map */
typedef H5FD_safe_virtual_file_map_t H5FD_safe_vfm_t;

/* The driver identification number, initialized at runtime */
static hid_t H5FD_SAFE_g = 0;

/* The description of a file belonging to this driver. The 'eoa' and 'eof'
 * determine the amount of hdf5 address space in use and the high-water mark
 * of the file (the current size of the underlying filesystem file). The
 * 'pos' value is used to eliminate file position updates when they would be a
 * no-op. Unfortunately we've found systems that use separate file position
 * indicators for reading and writing so the lseek can only be eliminated if
 * the current operation is the same as the previous operation.  When opening
 * a file the 'eof' will be set to the current file size, `eoa' will be set
 * to zero, 'pos' will be set to H5F_ADDR_UNDEF (as it is when an error
 * occurs), and 'op' will be set to H5F_OP_UNKNOWN.
 */
typedef struct H5FD_safe_t {
    H5FD_t          pub;    /* public stuff, must be first      */
    int             fd;     /* the filesystem file descriptor   */
    haddr_t         eoa;    /* end of allocated region          */
    haddr_t         eof;    /* end of file; current file size   */
    haddr_t         pos;    /* current file I/O position        */
    H5FD_file_op_t  op;     /* last operation                   */
    char            filename[H5FD_MAX_FILENAME_LEN];    /* Copy of file name from open operation */

    uint64_t file_id; /* the file id (a random one generated at file creation */
    uint64_t old_real_file_size;

    H5FD_safe_vfm_t * map;   /* map the virtual address space with the real file space */

    /* manage file alloacation */
    H5FD_safe_alloc_list_t * prot; /* protected area is are in file that cannot be used */
    H5FD_safe_alloc_list_t * free; /* area that ca be used to write new data */
    H5FD_safe_alloc_list_t * used; /* area that are used, this area can be freed to be reused later */

    /* which log are currently protected (i.e. the old valid log) */
    int selected_log;


#ifndef H5_HAVE_WIN32_API
    /* On most systems the combination of device and i-node number uniquely
     * identify a file.  Note that Cygwin, MinGW and other Windows POSIX
     * environments have the stat function (which fakes inodes)
     * and will use the 'device + inodes' scheme as opposed to the
     * Windows code further below.
     */
    dev_t           device;     /* file device number   */
#ifdef H5_VMS
    ino_t           inode[3];   /* file i-node number   */
#else
    ino_t           inode;      /* file i-node number   */
#endif /* H5_VMS */
#else
    /* Files in windows are uniquely identified by the volume serial
     * number and the file index (both low and high parts).
     *
     * There are caveats where these numbers can change, especially
     * on FAT file systems.  On NTFS, however, a file should keep
     * those numbers the same until renamed or deleted (though you
     * can use ReplaceFile() on NTFS to keep the numbers the same
     * while renaming).
     *
     * See the MSDN "BY_HANDLE_FILE_INFORMATION Structure" entry for
     * more information.
     *
     * http://msdn.microsoft.com/en-us/library/aa363788(v=VS.85).aspx
     */
    DWORD           nFileIndexLow;
    DWORD           nFileIndexHigh;
    DWORD           dwVolumeSerialNumber;
    
    HANDLE          hFile;      /* Native windows file handle */
#endif  /* H5_HAVE_WIN32_API */

    /* Information from properties set by 'h5repart' tool
     *
     * Whether to eliminate the family driver info and convert this file to
     * a single file.
     */
    hbool_t         fam_to_safe;
} H5FD_safe_t;



/**
 *
 * Log file structure is kept as simple as possible. Thus it is not optimized in size.
 *
 * The log file is compound by messages. Each message are contiguous and start at offset 0.
 * A message always start with a fixed length message header followed by free message data.
 * This header always start with 2 64 bits little-endian integers, the first one is the
 * message type and the second one is the size of free message data. The rest of the header
 * is depend on message type. See the following structure declaration to know the
 * interpretation of remaining data.
 *
 **/

/* any message struct, used to read any message header, this not a real message*/
typedef struct H5FD_safe_log_msg_any_t {
	int64_t type;
	uint64_t file_id;
	uint64_t log_id;
	int64_t check_sum;
	/** ensure zeros on not used byte **/
} H5FD_safe_log_msg_any_t;

/* any message struct, used to read any message header, this not a real message*/
typedef struct H5FD_safe_log_msg_extend_log_t {
	int64_t type;
	uint64_t file_id;
	uint64_t log_id;
	int64_t check_sum;
	int64_t next_addr;
	int64_t size;
	/** ensure zeros on not used byte **/
} H5FD_safe_log_msg_extend_log_t;

/* write message store write operation, data that must be written and there location of this data */
typedef struct H5FD_safe_log_msg_write_t {
	int64_t type;
	uint64_t file_id;
	uint64_t log_id;
	int64_t check_sum;
	int64_t size;
	int64_t voffset;
	int64_t roffset;
} H5FD_safe_log_msg_write_t;

/* truncate message store truncate operation */
typedef struct H5FD_safe_log_msg_truncate_t {
	int64_t type;
	uint64_t file_id;
	uint64_t log_id;
	int64_t check_sum;
	int64_t truncate_to;
} H5FD_safe_log_msg_truncate_t;

/* close operation store close operation */
typedef struct H5FD_safe_log_msg_close_t {
	int64_t type;
	uint64_t file_id;
	uint64_t log_id;
	int64_t check_sum;
} H5FD_safe_log_msg_close_t;

/* union of all messages type, this define the size of message header */
typedef union H5FD_safe_log_msg_t {
	int64_t type;
	H5FD_safe_log_msg_any_t m_any;
	H5FD_safe_log_msg_write_t m_write;
	H5FD_safe_log_msg_truncate_t m_trunk;
	H5FD_safe_log_msg_close_t m_close;
	H5FD_safe_log_msg_extend_log_t m_extend_log;
} H5FD_safe_log_msg_t;


/* list of message type and forseen message type */
typedef enum H5FD_safe_mesage_type_t {
	H5FD_SAFE_MSG_NONE = 0,					/* NO USED */
	H5FD_SAFE_MSG_WRITE,					/* write operation */
	H5FD_SAFE_MSG_WRITE_IN_FILE,			/* write in file, not used yet, but it is possible to
											   write new data directly in file without corrupting
											   the hdf5, when this data are wrote after the end of
											   file when it was opened */
	H5FD_SAFE_MSG_WRITE_IN_FILE_DONE,    /* not used yet see previous comment */
	H5FD_SAFE_MSG_CLOSE_FILE,			/* mark file close to validate log */
	H5FD_SAFE_MSG_TRUNCATE,				/* truncate masage */
	H5FD_SAFE_MSG_EXTEND_LOG,
	H5FD_SAFE_MSG_LAST
} H5FD_safe_mesage_type_t;

/* char for debuging purpose */
static char const * const s_mesage_type[] = {
		"H5FD_SAFE_MSG_NONE",
		"H5FD_SAFE_MSG_WRITE",
		"H5FD_SAFE_MSG_WRITE_IN_FILE",
		"H5FD_SAFE_MSG_WRITE_IN_FILE_DONE",
		"H5FD_SAFE_MSG_CLOSE_FILE",
		"H5FD_SAFE_MSG_TRUNCATE",
		"H5FD_SAFE_MSG_LAST",
};





/*
 * These macros check for overflow of various quantities.  These macros
 * assume that HDoff_t is signed and haddr_t and size_t are unsigned.
 *
 * ADDR_OVERFLOW:   Checks whether a file address of type `haddr_t'
 *                  is too large to be represented by the second argument
 *                  of the file seek function.
 *
 * SIZE_OVERFLOW:   Checks whether a buffer size of type `hsize_t' is too
 *                  large to be represented by the `size_t' type.
 *
 * REGION_OVERFLOW: Checks whether an address and size pair describe data
 *                  which can be addressed entirely by the second
 *                  argument of the file seek function.
 */
#define MAXADDR (((haddr_t)1<<(8*sizeof(HDoff_t)-1))-1)
#define ADDR_OVERFLOW(A)    (HADDR_UNDEF==(A) || ((A) & ~(haddr_t)MAXADDR))
#define SIZE_OVERFLOW(Z)    ((Z) & ~(hsize_t)MAXADDR)
#define REGION_OVERFLOW(A,Z)    (ADDR_OVERFLOW(A) || SIZE_OVERFLOW(Z) ||    \
                                 HADDR_UNDEF==(A)+(Z) ||                    \
                                (HDoff_t)((A)+(Z))<(HDoff_t)(A))


/* Prototypes */
static H5FD_t *H5FD_safe_open(const char *name, unsigned flags, hid_t fapl_id,
            haddr_t maxaddr);
static herr_t H5FD_safe_close(H5FD_t *_file);

static int64_t H5FD_safe_alloc(H5FD_safe_t * file, int64_t size);

static int H5FD_safe_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t H5FD_safe_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD_safe_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t H5FD_safe_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD_safe_get_eof(const H5FD_t *_file);
static herr_t  H5FD_safe_get_handle(H5FD_t *_file, hid_t fapl, void** file_handle);
static herr_t H5FD_safe_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr,
            size_t size, void *buf);
static herr_t H5FD_safe_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr,
            size_t size, const void *buf);
static herr_t H5FD_safe_truncate(H5FD_t *_file, hid_t dxpl_id, hbool_t closing);


static H5FD_safe_vfm_t * H5FD_safe_vfm_new(int64_t size);
static void H5FD_safe_vfm_delete(H5FD_safe_vfm_t * ths);
static H5FD_safe_vfm_t * H5FD_safe_vfm_find_node(H5FD_safe_vfm_t * ths, int64_t offset);
static H5FD_safe_vfm_t * H5FD_safe_vfm_split(H5FD_safe_vfm_t * ths, int64_t offset);
static H5FD_safe_vfm_t * H5FD_safe_vfm_insert(H5FD_safe_t * file, H5FD_safe_vfm_t * ths, int64_t voffset, int64_t roffset, int64_t size);
static H5FD_safe_vfm_t * H5FD_safe_vfm_truncate(H5FD_safe_vfm_t * ths, size_t size);
static H5FD_safe_vfm_t * H5FD_safe_vfm_grow(H5FD_safe_vfm_t * ths, size_t size);
static H5FD_safe_vfm_t * H5FD_safe_vfm_remove(H5FD_safe_vfm_t * ths, H5FD_safe_vfm_t * first, H5FD_safe_vfm_t * last);
static void H5FD_safe_vfm_print(H5FD_safe_vfm_t * ths);
static H5FD_safe_vfm_t * H5FD_safe_vfm_extract(H5FD_safe_vfm_t * ths, int64_t offset, int64_t size);


//static herr_t H5FD_safe_replay_log(char const * filename, char const * log_filename);

static herr_t H5FD_safe_virtual_truncate(H5FD_safe_t * file, size_t size);

static const H5FD_class_t H5FD_safe_g = {
    "safe",                     /* name                 */
    MAXADDR,                    /* maxaddr              */
    H5F_CLOSE_WEAK,             /* fc_degree            */
    NULL,                       /* sb_size              */
    NULL,                       /* sb_encode            */
    NULL,                       /* sb_decode            */
    0,                          /* fapl_size            */
    NULL,                       /* fapl_get             */
    NULL,                       /* fapl_copy            */
    NULL,                       /* fapl_free            */
    0,                          /* dxpl_size            */
    NULL,                       /* dxpl_copy            */
    NULL,                       /* dxpl_free            */
    H5FD_safe_open,             /* open                 */
    H5FD_safe_close,            /* close                */
    H5FD_safe_cmp,              /* cmp                  */
    H5FD_safe_query,            /* query                */
    NULL,                       /* get_type_map         */
    NULL,                       /* alloc                */
    NULL,                       /* free                 */
    H5FD_safe_get_eoa,          /* get_eoa              */
    H5FD_safe_set_eoa,          /* set_eoa              */
    H5FD_safe_get_eof,          /* get_eof              */
    H5FD_safe_get_handle,       /* get_handle           */
    H5FD_safe_read,             /* read                 */
    H5FD_safe_write,            /* write                */
    NULL,                       /* flush                */
    H5FD_safe_truncate,         /* truncate             */
    NULL,                       /* lock                 */
    NULL,                       /* unlock               */
    H5FD_FLMAP_DICHOTOMY        /* fl_map               */
};

/* Declare a free list to manage the H5FD_safe_t struct */
H5FL_DEFINE_STATIC(H5FD_safe_t);


static herr_t
H5FD_safe_write_log_msg(H5FD_safe_t * file, H5FD_safe_log_msg_t * ths) {
//	if(file->log_handler.free_log_size < (sizeof(H5FD_safe_log_msg_t)*2)) {
//		H5FD_safe_log_msg_t msg;
//		memset(&msg, 0, sizeof(H5FD_safe_log_msg_t));
//		int64_t laddr = H5FD_safe_alloc(file, 8192);
//		msg.m_extend_log.next_addr = laddr;
//		msg.m_extend_log.size = 8192;
//		msg.m_extend_log.type = H5FD_SAFE_MSG_EXTEND_LOG;
//		msg.m_extend_log.file_id = file->file_id;
//		msg.m_extend_log.log_id = file->log_fd;
//		msg.m_extend_log.check_sum = 0;
//		msg.m_extend_log.check_sum = H5_checksum_fletcher32(&msg, sizeof(H5FD_safe_log_msg_t));
//
//		HDlseek(file->fd, file->log_handler.current_file_offset, SEEK_SET);
//		HDwrite(file->fd, &msg, sizeof(H5FD_safe_log_msg_t));
//
//    	file->log_handler.current_file_offset = laddr;
//    	file->log_handler.free_log_size = 8192;
//	}
//
//	HDlseek(file->fd, file->log_handler.current_file_offset, SEEK_SET);
//	HDwrite(file->fd, ths, sizeof(H5FD_safe_log_msg_t));
//	file->log_handler.current_file_offset += sizeof(H5FD_safe_log_msg_t);
//	file->log_handler.free_log_size -= sizeof(H5FD_safe_log_msg_t);
//
//	return 0;

}



static int64_t
H5FD_safe_alloc(H5FD_safe_t * file, int64_t size) {
	printf("H5FD_safe_alloc(%ld)\n", size);
	//H5FD_safe_alloc_list_print(file->free, "Before free");
	//H5FD_safe_alloc_list_print(file->used, "Before used");


	H5FD_safe_alloc_list_t * cur = file->free;
	while(cur != NULL) {
		if(cur->size >= size)
			break;
		cur = cur->next;
	}

	if(cur == NULL)
		return 0;

	if(cur->size == size) {
		file->free = H5FD_safe_alloc_list_remove(file->free, cur);
		file->used = H5FD_safe_alloc_list_add(file->used, cur);

		//H5FD_safe_alloc_list_print(file->free, "After free");
		//H5FD_safe_alloc_list_print(file->used, "After used");


		return cur->offset;
	} else {
		H5FD_safe_alloc_list_t * x;
		int64_t offset = cur->offset;
		cur->offset += size;
		cur->size -= size;
		x = H5FD_safe_alloc_list_new_node(offset, size);
		file->used = H5FD_safe_alloc_list_add(file->used, x);

		//H5FD_safe_alloc_list_print(file->free, "After free");
		//H5FD_safe_alloc_list_print(file->used, "After used");


		return  offset;
	}

}

static void
H5FD_safe_free(H5FD_safe_t * file, int64_t offset, int64_t size) {
	H5FD_safe_alloc_list_t * cur;
	printf("H5FD_safe_free(%ld,%ld)\n", offset, size);
	//H5FD_safe_alloc_list_print(file->free, "Before free");
	//H5FD_safe_alloc_list_print(file->used, "Before used");


	cur = file->used;
	while(cur != NULL) {
		int64_t min = MAX(offset, cur->offset);
		int64_t max = MIN(offset + size, cur->offset + cur->size);

		/* if current used overlaps freed data */
		if(min < max) {
			H5FD_safe_alloc_list_t * node;

			printf("found used overlaps free [%ld,%ld], [%ld,%ld], [%ld,%ld]\n",
					offset, offset + size, cur->offset, cur->offset+ cur->size, min, max);

			node = H5FD_safe_alloc_list_new_node(
						min, max - min);
				file->free = H5FD_safe_alloc_list_add(file->free, node);


			file->used = H5FD_safe_alloc_list_remove(file->used, cur);

			if(min > cur->offset) {
				int64_t xoffset = cur->offset;
				int64_t xsize = min - cur->offset;
				node = H5FD_safe_alloc_list_new_node(xoffset, xsize);
				file->used = H5FD_safe_alloc_list_add(file->used, node);
			}

			if(max < cur->offset + cur->size) {
				int64_t xoffset = max;
				int64_t xsize = cur->offset + cur->size - max;
				node = H5FD_safe_alloc_list_new_node(xoffset, xsize);
				file->used = H5FD_safe_alloc_list_add(file->used, node);
			}

			node = cur->next;
			free(cur);
			cur = node;

		} else {
			cur = cur->next;
		}
	}

	//H5FD_safe_alloc_list_print(file->free, "After free");
	//H5FD_safe_alloc_list_print(file->used, "After used");

}

static H5FD_safe_vfm_t *
H5FD_safe_vfm_extract(H5FD_safe_vfm_t * ths, int64_t offset, int64_t size) {
	H5FD_safe_vfm_t * cur_src, * cur_ret;
	//printf("H5FD_safe_vfm_extract(offset = %ld, size = %ld)\n", offset, size);

	cur_src = ths;
	cur_ret = NULL;
	while(cur_src != NULL) {
		int64_t x_start = MAX(offset, cur_src->voffset);
		int64_t x_end = MIN(offset+size, cur_src->voffset + cur_src->size);

		if(x_end - x_start > 0) {
			/* create new node */
			H5FD_safe_vfm_t * x = (H5FD_safe_vfm_t *) malloc(sizeof(H5FD_safe_vfm_t));
			if (cur_src->roffset > 0) {
				x->roffset = cur_src->roffset + x_start - cur_src->voffset;
			} else {
				x->roffset = -1;
			}
			x->voffset = x_start;
			x->size = x_end - x_start;
			x->prev = cur_ret;
			x->next = NULL;
			if(cur_ret != NULL) {
				cur_ret->next = x;
			}
			cur_ret = x;
		}
		cur_src = cur_src->next;
	}

	if (cur_ret != NULL) {
		while (cur_ret->prev != NULL) {
			cur_ret = cur_ret->prev;
		}
	}

	return cur_ret;

}

static H5FD_safe_vfm_t *
H5FD_safe_vfm_cleanup(H5FD_safe_vfm_t * ths) {
	H5FD_safe_vfm_t * i = ths;
	printf("H5FD_safe_alloc_list_cleanup(%p)\n", ths);
	H5FD_safe_alloc_list_print(ths, "anonymous");
	while(i != NULL) {
		H5FD_safe_vfm_t * j = i->next;
		while(j != NULL) {
			if (i->roffset > 0 && j->roffset > 0) {
				if ((i->voffset + i->size) == j->voffset
						&& (i->roffset + i->size == j->roffset)) {
					i->size = j->voffset + j->size - i->voffset;
					i->next = j->next;
					if (j->next != NULL)
						j->next->prev = i;

					free(j);
					j = i->next;
				} else {
					j = j->next;
				}
			} else if(i->roffset < 0 && j->roffset < 0) {
				if ((i->voffset + i->size) == j->voffset) {
					i->size = j->voffset + j->size - i->voffset;
					i->next = j->next;
					if (j->next != NULL)
						j->next->prev = i;

					free(j);
					j = i->next;
				} else {
					j = j->next;
				}
			} else {
				j = j->next;
			}
		}
		i = i->next;
	}
	return ths;
}


static void
H5FD_safe_vfm_print(H5FD_safe_vfm_t * ths) {
	H5FD_safe_vfm_t * cur = ths;
	printf("MAP BEGIN\n");
	while(cur != NULL) {
		char const * s = NULL;

		printf("start = %ld, end = %ld, poffset = %ld, size = %ld, cur = %p, next = %p, prev = %p\n",
				cur->voffset, cur->voffset + cur->size, cur->roffset, cur->size, cur, cur->next, cur->prev);

		if(cur->next != NULL) {
			if(cur->voffset + cur->size != cur->next->voffset)
				printf("WARNING: inconsistent file map -> vofset are inconsistant\n");
		}

		if(cur->next != NULL) {
			if(cur != cur->next->prev)
				printf("WARNING: inconsistent file map -> pointer prev are inconsistent\n");
		}

		cur = cur->next;
	}
	printf("MAP END\n");

}


static H5FD_safe_vfm_t *
H5FD_safe_vfm_new(int64_t size) {
	H5FD_safe_vfm_t * ret_value;
	if(size == 0)
		return NULL;
	ret_value = (H5FD_safe_vfm_t*)malloc(sizeof(H5FD_safe_vfm_t));
	if(ret_value == NULL)
		return NULL;

	if(size == 0)
		return NULL;

	ret_value->voffset = 0;
	ret_value->roffset = 0;
	ret_value->size = size;
	ret_value->next = NULL;
	ret_value->prev = NULL;

	//H5FD_safe_vfm_print(ret_value);

	return ret_value;
}

static void
H5FD_safe_vfm_delete(H5FD_safe_vfm_t * ths) {
	H5FD_safe_vfm_remove(ths, ths, NULL);
}

static int
H5FD_safe_vfm_count(H5FD_safe_vfm_t * ths) {
	int ret = 0;
	while(ths != NULL) {
		++ret;
		ths = ths->next;
	}
}

static H5FD_safe_vfm_t *
H5FD_safe_vfm_find_node(H5FD_safe_vfm_t * ths, int64_t offset) {
	H5FD_safe_vfm_t * cur = ths;
	while(cur != NULL) {
		if(cur->voffset <= offset && (cur->voffset + cur->size) > offset)
			return cur;
		cur = cur->next;
	}
	return NULL;
}

static H5FD_safe_vfm_t *
H5FD_safe_vfm_split(H5FD_safe_vfm_t * ths, int64_t offset) {
	H5FD_safe_vfm_t * node = H5FD_safe_vfm_find_node(ths, offset);
	if(node == NULL)
		return ths;

	//printf("voffset = %ld, offset = %ld\n", node->voffset, offset);

	if (node->voffset != offset) {
		H5FD_safe_vfm_t * insert = (H5FD_safe_vfm_t *)malloc(sizeof(H5FD_safe_vfm_t));;
		if(insert == NULL)
			return ths;

		/* create new node */
		insert->voffset = offset;
		if (node->roffset > 0) {
			insert->roffset = node->roffset + offset - node->voffset;
		} else {
			insert->roffset = -1;
		}
		insert->size = node->voffset + node->size - offset;
		insert->next = node->next;
		insert->prev = node;

		if(node->next != NULL)
			node->next->prev = insert;

		/* update old node */
		node->next = insert;
		node->size = offset - node->voffset;


	}

	return ths;


}

static H5FD_safe_vfm_t *
H5FD_safe_vfm_remove(H5FD_safe_vfm_t * ths, H5FD_safe_vfm_t * first, H5FD_safe_vfm_t * last) {
	H5FD_safe_vfm_t * ret_value, * cur;

	//printf("H5FD_safe_vfm_remove(%p,%p,%p)\n", ths, first, last);

	//H5FD_safe_vfm_print(ths);

	if(ths == NULL)
		return ths;

	if(first == NULL)
		return ths;

	if(first == last)
		return ths;

	if(first->prev == NULL) {
		ret_value = last;
	} else {
		ret_value = ths;
		first->prev->next = last;
	}

	if(last != NULL)
		last->prev = first->prev;

	cur = first;

	while (cur != last && cur != NULL) {
		H5FD_safe_vfm_t * tmp = cur;
		cur = cur->next;
		free(tmp);
	}

	if(cur == NULL && last != NULL)
		printf("WARNING: inconsistent remove\n");

	//H5FD_safe_vfm_print(ret_value);

	return ret_value;
}

static H5FD_safe_vfm_t *
H5FD_safe_vfm_insert(H5FD_safe_t * file, H5FD_safe_vfm_t * ths, int64_t voffset, int64_t roffset, int64_t size) {

	H5FD_safe_vfm_t * node_first;
	H5FD_safe_vfm_t * node_last;
	H5FD_safe_vfm_t * cur;

	//printf("H5FD_safe_vfm_insert(voffset=%ld,roffset=%ld,size=%ld)\n", voffset, roffset, size);

	/**
	 * trick to have plain node on voffset and voffset+size;
	 **/
	ths = H5FD_safe_vfm_split(ths, voffset);
	ths = H5FD_safe_vfm_split(ths, voffset + size);

	if(size <= 0)
		return ths;

	node_first = H5FD_safe_vfm_find_node(ths, voffset);
	node_last = H5FD_safe_vfm_find_node(ths, voffset + size);

	//H5FD_safe_vfm_print(ths);

	HDassert(node_first != NULL);
	HDassert(node_first->voffset == voffset);
	HDassert(node_last->voffset == voffset + size);

	/* free corresponding nodes */
	cur = node_first;
	while(cur != node_last) {
		if(cur->roffset > 0) {
			/** free only unprotected data, because protected data never go in file->used or file->free **/
			H5FD_safe_free(file, cur->roffset, cur->size);
		}
		cur = cur->next;
	}

	ths = H5FD_safe_vfm_remove(ths, node_first->next, node_last);

	/** update node_first **/
	node_first->roffset = roffset;
	node_first->size = size;

	ths = H5FD_safe_vfm_cleanup(ths);
	//H5FD_safe_vfm_print(ths);

	return ths;

}

/**
 * Replay log file, this used on close file and on open as needed.
 *
 * Replay have 3 pass :
 *
 *  1. read replay file and check if it end with a close message.
 *     if not, the replay is dropped, all update are lost.
 *
 *  2. rebuild the virtual file map, to map log file data into
 *     final file data
 *
 *  3. read map and copy log into file.
 *
 *  If this operation is interrupted no data will be lost, Replay
 *  will be remade next time the file is open and file will be in
 *  valid state.
 *
 **/
//static herr_t
//H5FD_safe_replay_log(char const * filename, char const * log_filename) {
//
//	int fd;
//	int log_fd;
//	h5_stat_t sb;
//	H5FD_safe_vfm_t * map;
//	H5FD_safe_log_msg_t hdr;
//	int64_t max_write_size = 0;
//	int valid_log = 0;
//
//	printf("Replay journal for '%s' with '%s'\n", filename, log_filename);
//
//	fd = HDopen(filename, O_RDWR, 0666);
//	if(fd < 0) {
//		printf("fail to open '%s'\n", filename);
//		return -1;
//	}
//
//	log_fd = HDopen(log_filename, O_RDONLY, 0444);
//	if(log_fd < 0) {
//		printf("fail to open '%s'\n", log_filename);
//		return -1;
//	}
//
//	HDfstat(fd, &sb);
//	map = H5FD_safe_vfm_new(sb.st_size);
//
//	HDlseek(log_fd, 0, SEEK_SET);
//
//	/**
//	 * Step 1. check if log file is valid.
//	 **/
//	while(1) {
//		if(HDread(log_fd, &hdr, sizeof(H5FD_safe_log_msg_t)) < (ssize_t)sizeof(H5FD_safe_log_msg_t)) {
//			printf("fail to read header\n");
//			/* invalid log */
//			break;
//		}
//
//		if(hdr.type == H5FD_SAFE_MSG_WRITE) {
//			if(max_write_size < hdr.m_any.size) {
//				max_write_size = hdr.m_any.size;
//			}
//
//			printf("header type = %s, size = %d\n", s_mesage_type[hdr.m_write.type], hdr.m_write.size);
//
//		} else if (hdr.type == H5FD_SAFE_MSG_CLOSE_FILE) {
//			printf("header type = %s, size = %d\n", s_mesage_type[hdr.m_any.type], hdr.m_any.size);
//			valid_log = 1;
//			break;
//		} else if (hdr.type == H5FD_SAFE_MSG_TRUNCATE) {
//			printf("header type = %s, size = %d\n", s_mesage_type[hdr.m_any.type], hdr.m_any.size);
//
//			/** continue **/
//		} else {
//			/** unknown type => invalid log **/
//			break;
//		}
//
//
//		/* goto next message */
//		HDlseek(log_fd, hdr.m_any.size, SEEK_CUR);
//
//	}
//
//	/**
//	 * if log is valid go rebuild virtual file map.
//	 **/
//
//	if(valid_log == 1) {
//
//		/** go back to begin **/
//		HDlseek(log_fd, 0, SEEK_SET);
//
//		while(1) {
//			if(HDread(log_fd, &hdr, sizeof(hdr)) < sizeof(hdr)) {
//				/* should not fail ... */
//				return -1;
//			}
//
//			if(hdr.type == H5FD_SAFE_MSG_WRITE) {
//				map = H5FD_safe_vfm_grow(map, (size_t)(hdr.m_write.voffset + hdr.m_write.size));
//				map = H5FD_safe_vfm_insert(map, hdr.m_write.voffset, HDlseek(log_fd, 0, SEEK_CUR), hdr.m_write.size);
//			} else if (hdr.type == H5FD_SAFE_MSG_CLOSE_FILE) {
//				break;
//			} else if (hdr.type == H5FD_SAFE_MSG_TRUNCATE) {
//				map = H5FD_safe_vfm_truncate(map, (size_t)hdr.m_trunk.truncate_to);
//			} else {
//				/** unexpected someone are writing log file ? **/
//				return -1;
//			}
//
//			/* goto next message */
//			HDlseek(log_fd, hdr.m_any.size, SEEK_CUR);
//
//		}
//
//		/* replay MAP */
//		printf("Replay MAP\n");
//		H5FD_safe_vfm_print(map);
//
//		/**
//		 * write operation.
//		 **/
//
//		{
//			H5FD_safe_vfm_t * cur = map;
//
//			/** Allocate the maximum needed write size **/
//			unsigned char * buf = (unsigned char *)malloc((size_t)max_write_size);
//			if(buf == NULL)
//				return -1;
//
//
//			while(cur != NULL) {
//				if(cur->file_source == H5FD_SAFE_SRC_LOG) {
//
//					/* read from log */
//					HDlseek(log_fd, cur->roffset, SEEK_SET);
//					if(HDread(log_fd, buf, (size_t)cur->size) < 0)
//						return -1;
//
//					/* write to hdf5 file */
//
//					printf("H5FD_safe_write(FILE,%ld,%ld)\n", cur->voffset, cur->size);
//					HDlseek(fd, cur->voffset, SEEK_SET);
//					if(HDwrite(fd, buf, (size_t)cur->size) < 0)
//						return -1;
//				}
//
//				cur = cur->next;
//			}
//
//			free(buf);
//		}
//
//		fsync(fd);
//		close(fd);
//		close(log_fd);
//
//		H5FD_safe_vfm_delete(map);
//
//		/** now we can trash log file **/
//	} else {
//		return -1;
//	}
//
//	return 0;
//
//}

/** return 0 if log is valid or -1 if log is invalid **/
static herr_t
H5FD_safe_check_log(int fd, uint64_t file_id, uint64_t log_id, uint64_t addr) {
	H5FD_safe_log_msg_t msg;
	int valid_log = 0;

	HDlseek(fd, addr, SEEK_SET);

	/**
	 * Step 1. check if log file is valid.
	 **/
	while(1) {
		int64_t ref_checksum;
		int64_t tmp_checksum;
		if(HDread(fd, &msg, sizeof(H5FD_safe_log_msg_t)) < (ssize_t)sizeof(H5FD_safe_log_msg_t)) {
			printf("fail to read header\n");
			/* invalid log */
			break;
		}

		/* check message checksum */
		ref_checksum = msg.m_any.check_sum;
		msg.m_any.check_sum = 0;
		tmp_checksum = H5_checksum_fletcher32(&msg, sizeof(H5FD_safe_log_msg_t));
		if(tmp_checksum != ref_checksum) {
			printf("Error message checksum doesn't match\n");
			break;
		}

		if(file_id != msg.m_any.file_id) {
			printf("file_id doesn't match\n");
			break;
		}

		if(log_id != msg.m_any.log_id) {
			printf("log_id doesn't match\n");
			break;
		}

		if(msg.type == H5FD_SAFE_MSG_WRITE) {
			printf("header type = %s, size = %d\n", s_mesage_type[msg.m_write.type], msg.m_write.size);
		} else if (msg.type == H5FD_SAFE_MSG_CLOSE_FILE) {
			//printf("header type = %s, size = %d\n", s_mesage_type[msg.m_any.type], msg.m_any.size);
			valid_log = 1;
			break;
		} else if (msg.type == H5FD_SAFE_MSG_TRUNCATE) {
			//printf("header type = %s, size = %d\n", s_mesage_type[msg.m_any.type], msg.m_any.size);

			/** continue **/
		} else if (msg.type == H5FD_SAFE_MSG_EXTEND_LOG){
			/** jump to the next message **/
			HDlseek(fd, msg.m_extend_log.next_addr, SEEK_SET);
			//printf("header type = %s, size = %d\n", s_mesage_type[msg.m_any.type], msg.m_any.size);
		} else {
			/** unknown type => invalid log **/
			break;
		}

	}

	if(valid_log == 0)
		return -1;

	return 0;

}

static ssize_t H5FD_safe_virtual_write(H5FD_safe_t * file, void const * buf, int64_t addr, size_t size) {
	ssize_t ret_value = -1;
	int64_t raddr;

	/** write to log file **/
	//H5FD_safe_log_msg_t header;

	printf("H5FD_safe_virtual_write(%ld,%ld)\n", addr, size);

	//HDassert(file->log_fd != -1);

	//H5FD_safe_vfm_print(file->map);

	/** allocate file area **/
	raddr = H5FD_safe_alloc(file, (int64_t)size);

	/* write data to this area */
	HDlseek(file->fd, raddr, SEEK_SET);
	do {
		ret_value = HDwrite(file->fd, buf, size);
	} while (-1 == ret_value && EINTR == errno);
	//printf("writed data : %d %d\n", size, ret_value);

	//H5FD_safe_vfm_print(file->map);
	file->map = H5FD_safe_vfm_grow(file->map, (size_t) (addr + size));
	//printf("Grow:\n");
	//H5FD_safe_vfm_print(file->map);
	file->map = H5FD_safe_vfm_insert(file, file->map, addr, raddr,
			(int64_t) size);
	//H5FD_safe_vfm_print(file->map);

	return ret_value;

}

static ssize_t
H5FD_safe_virtual_read(H5FD_safe_t * file, void * buf, int64_t addr, size_t const size) {

	ssize_t ret_value = 0;
	unsigned char * xbuf;
	int64_t voffset;
	H5FD_safe_vfm_t * cur;
	H5FD_safe_vfm_t * ext;

	voffset = addr;

	/* extract current read from map */
	ext = H5FD_safe_vfm_extract(file->map, voffset, (int64_t)size);
	//H5FD_safe_vfm_print(file->map);
	//printf("Extracted:\n");
	//H5FD_safe_vfm_print(ext);

	cur = ext;
	xbuf = (unsigned char *)buf;
	while(cur != NULL) {
		if(cur->roffset > 0) {
			int64_t read_size = 0;

			if(HDlseek(file->fd, cur->roffset, SEEK_SET) < 0)
				return -1;

			printf("H5FD_safe_virtual_read(FILE,%p,%ld,%ld)\n", xbuf, cur->roffset, cur->size);

			do {
				(read_size = (int64_t)HDread(file->fd, xbuf, cur->size));
	        } while(-1 == read_size && EINTR == errno);

			//printf("Readed %ld %d\n", read_size, errno);
			if(read_size < cur->size) {
				return ret_value + read_size;
			}

			xbuf += read_size;
			ret_value += read_size;

		}

		cur = cur->next;
	}

	H5FD_safe_vfm_delete(ext);

	return ret_value;

}

static H5FD_safe_vfm_t *
H5FD_safe_vfm_truncate(H5FD_safe_vfm_t * ths, size_t size) {

	H5FD_safe_vfm_t * node = H5FD_safe_vfm_find_node(ths,
			(int64_t) size);
	if (node != NULL) {
		/** reduce size **/
		ths = H5FD_safe_vfm_split(ths, (int64_t) size);
		node = H5FD_safe_vfm_find_node(ths, (int64_t) size);
		ths = H5FD_safe_vfm_remove(ths, node, NULL);
	} else {
		/** grow size **/
		ths = H5FD_safe_vfm_grow(ths, size);
	}

	return ths;
}

static H5FD_safe_vfm_t *
H5FD_safe_vfm_grow(H5FD_safe_vfm_t * ths, size_t size) {


	if (ths == NULL) {
		/* the lis is empty, grow size */
		ths = (H5FD_safe_vfm_t *) malloc(sizeof(H5FD_safe_vfm_t));
		ths->voffset = 0;
		ths->roffset = -1;
		ths->next = NULL;
		ths->prev = NULL;
		ths->size = (int64_t)size;
	} else {
		H5FD_safe_vfm_t * node;

		/* find last node */
		node = ths;
		while (node->next != NULL) {
			node = node->next;
		}

		if (node->voffset + node->size < (int64_t) size) {
			node->next = (H5FD_safe_vfm_t *) malloc(sizeof(H5FD_safe_vfm_t));
			node->next->voffset = node->voffset + node->size;
			node->next->roffset = -1;
			node->next->size = (int64_t) size - node->next->voffset;
			node->next->next = NULL;
			node->next->prev = node;
		}

	}

	return ths;
}

static void
H5FD_safe_vfm_serialize(H5FD_safe_vfm_t * ths, uint64_t file_id, uint64_t log_id, char ** data, uint64_t * count, int64_t * size) {
	H5FD_safe_vfm_t * cur;
	int i;
	H5FD_safe_vfm_serialize_header_t * hdr;
	H5FD_safe_vfm_serialize_node_t * nodes;
	uint64_t * data_check_sum;
	uint64_t node_count = 0;

	printf("H5FD_safe_vfm_serialize(%lu,%lu)\n", file_id, log_id);
	//H5FD_safe_vfm_print(ths);

	if(data == NULL || size == NULL)
		return;

	cur = ths;
	while(cur != NULL) {
		++node_count;
		cur = cur->next;
	}

	*count = node_count;
	*size = sizeof(H5FD_safe_vfm_serialize_node_t) * node_count + sizeof(uint64_t);
	*data = (char *)malloc((uint64_t)*size);
	nodes = (H5FD_safe_vfm_serialize_node_t*)&((*data)[0]);
	data_check_sum = (uint64_t*)&((*data)[sizeof(H5FD_safe_vfm_serialize_node_t) * node_count]);

	cur = ths;
	i = 0;
	while(cur != NULL) {
		nodes[i].voffset = cur->voffset;
		nodes[i].roffset = cur->roffset;
		nodes[i].size = cur->size;
		++i;
		cur = cur->next;
	}

	/** make the final checksum **/
	*data_check_sum = H5_checksum_fletcher32(*data, sizeof(H5FD_safe_vfm_serialize_node_t) * node_count);

}

static herr_t
H5FD_safe_vfm_unserialize(int fd, int64_t offset, uint64_t count, H5FD_safe_virtual_file_map_t ** ths, int64_t * size) {
	H5FD_safe_vfm_t ** cur, * prev;
	int i;
	uint64_t hdr_check_sum;
	uint64_t tmp_check_sum;

	H5FD_safe_vfm_serialize_node_t * nodes;

	printf("H5FD_safe_vfm_unserialize()\n");

	HDlseek(fd, offset, SEEK_SET);

	*size = (int64_t)(sizeof(H5FD_safe_vfm_serialize_node_t) * count + sizeof(uint64_t));
	nodes = (H5FD_safe_vfm_serialize_node_t *)malloc(sizeof(H5FD_safe_vfm_serialize_node_t) * count);
	if(HDread(fd, nodes, sizeof(H5FD_safe_vfm_serialize_node_t) * count) < 0)
		return -1;
	if(HDread(fd, &hdr_check_sum, sizeof(uint64_t)) < 0)
		return -1;
	tmp_check_sum = H5_checksum_fletcher32(nodes, sizeof(H5FD_safe_vfm_serialize_node_t) * count);
	if(tmp_check_sum != hdr_check_sum) {
		return -1;
	}

	cur = ths;
	prev = NULL;
	for(i = 0 ; i < count; ++i) {
		H5FD_safe_virtual_file_map_t * node = (H5FD_safe_virtual_file_map_t *)malloc(sizeof(H5FD_safe_virtual_file_map_t));
		node->voffset = nodes[i].voffset;
		node->roffset = nodes[i].roffset;
		node->size = nodes[i].size;
		node->prev = prev;
		prev = node;
		*cur = node;
		cur = &(node->next);
	}

	*cur = NULL;

	free(nodes);

	//printf("Unserialized map:\n");
	//H5FD_safe_vfm_print(*ths);

	return 0;

}

static uint64_t
H5FD_safe_vfm_file_size(H5FD_safe_vfm_t const * ths) {
	H5FD_safe_vfm_t * cur = ths;
	if (cur != NULL) {
		while (cur->next != NULL) {
			cur = cur->next;
		}
		return (uint64_t)(cur->voffset + cur->size);
	} else {
		return 0;
	}
}

static uint64_t
H5FD_safe_vfm_file_real_size(H5FD_safe_vfm_t const * ths) {
	uint64_t max = 0;
	while(ths != NULL) {
		if(max < (uint64_t)(ths->roffset + ths->size)) {
			max = (uint64_t)(ths->roffset + ths->size);
		}
		ths = ths->next;
	}
	return max;
}

static herr_t
H5FD_safe_load_log(H5FD_safe_t * file, H5FD_safe_log_header_t * log, H5FD_safe_virtual_file_map_t ** map, int64_t * size) {
	/** check first log **/
	uint64_t tmp_checksum;
	uint64_t ref_checksum = log->check_sum;

	log->check_sum = 0;
	tmp_checksum = H5_checksum_fletcher32(log, sizeof(H5FD_safe_log_header_t));
	if (tmp_checksum != ref_checksum)
		return -1;

	if(log->file_map_offset == 0)
		return -1;

	*map = NULL;
	if(H5FD_safe_vfm_unserialize(file->fd, log->file_map_offset, log->file_map_fragment_count, map, size) < 0)
		return -1;

	return 0;
}

static herr_t
H5FD_safe_log_select(H5FD_safe_t * file,
		H5FD_safe_log_header_t * log, H5FD_safe_virtual_file_map_t * map,
		int64_t size) {
	H5FD_safe_alloc_list_t * x;

	file->map = map;
	file->free = NULL;
	file->used = NULL;
	file->prot = NULL;

	/* Add headers as protected */
	x = H5FD_safe_alloc_list_new_node(0, sizeof(H5FD_safe_file_header_t));
	file->prot = H5FD_safe_alloc_list_add(file->prot, x);

		/* Add current map file as protected */
		x = H5FD_safe_alloc_list_new_node(
				log->file_map_offset, size);
		file->prot = H5FD_safe_alloc_list_add(file->prot, x);


	{
		/** add current allocated data **/
		H5FD_safe_virtual_file_map_t * cur = file->map;
		while (cur != NULL) {
			if (cur->roffset > 0) {
				x = H5FD_safe_alloc_list_new_node(cur->roffset, cur->size);
				file->prot = H5FD_safe_alloc_list_add(file->prot, x);
			}
			cur = cur->next;
		}
	}

	/* set free space */
	file->free = H5FD_safe_negative_mask(file->prot);
	H5FD_safe_alloc_list_print(file->prot, "Protected file sections");
	H5FD_safe_alloc_list_print(file->free, "Initial Free");

	return 0;
}

static herr_t
H5FD_safe_virtual_truncate(H5FD_safe_t * file, size_t size) {
	//H5FD_safe_log_msg_t msg;

	file->map = H5FD_safe_vfm_truncate(file->map, size);


	return 0;
//	msg.m_trunk.type = H5FD_SAFE_MSG_TRUNCATE;
//	msg.m_trunk.size = 0;
//	msg.m_trunk.truncate_to = (int64_t)size;
//
//	return (herr_t)HDwrite(file->log_fd, &msg, sizeof(H5FD_safe_log_msg_t));

}




/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_init_interface
 *
 * Purpose:     Initializes any interface-specific data or routines.
 *
 * Return:      Success:    The driver ID for the sec2 driver.
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_safe_init_interface(void)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(H5FD_safe_init())
} /* H5FD_safe_init_interface() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the sec2 driver.
 *              Failure:    Negative
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_safe_init(void)
{
    hid_t ret_value;            /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if(H5I_VFL != H5I_get_type(H5FD_SAFE_g))
        H5FD_SAFE_g = H5FD_register(&H5FD_safe_g, sizeof(H5FD_class_t), FALSE);

    /* Set return value */
    ret_value = H5FD_SAFE_g;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_safe_init() */


/*---------------------------------------------------------------------------
 * Function:    H5FD_safe_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     <none>
 *
 * Programmer:  Quincey Koziol
 *              Friday, Jan 30, 2004
 *
 *---------------------------------------------------------------------------
 */
void
H5FD_safe_term(void)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Reset VFL ID */
    H5FD_SAFE_g = 0;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5FD_safe_term() */


/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_safe
 *
 * Purpose:     Modify the file access property list to use the H5FD_SAFE
 *              driver defined in this source file.  There are no driver
 *              specific properties.
 *
 * Return:      SUCCEED/FAIL
 *
 * Programmer:  Robb Matzke
 *              Thursday, February 19, 1998
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_safe(hid_t fapl_id)
{
    H5P_genplist_t *plist;      /* Property list pointer */
    herr_t ret_value;

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", fapl_id);

    if(NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list")

    ret_value = H5P_set_driver(plist, H5FD_SAFE, NULL);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_safe() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_open
 *
 * Purpose:     Create and/or opens a file as an HDF5 file.
 *
 * Return:      Success:    A pointer to a new file data structure. The
 *                          public fields will be initialized by the
 *                          caller, which is always H5FD_open().
 *              Failure:    NULL
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD_safe_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5FD_safe_t     *file       = NULL;     /* sec2 VFD info            */
    int             fd          = -1;       /* File descriptor          */
    int             o_flags;                /* Flags for open() call    */
#ifdef H5_HAVE_WIN32_API
    struct _BY_HANDLE_FILE_INFORMATION fileinfo;
#endif
    h5_stat_t       sb;
    H5FD_t          *ret_value;             /* Return value             */

	H5FD_safe_virtual_file_map_t * map0 = NULL;
	int64_t size0;
	H5FD_safe_virtual_file_map_t * map1 = NULL;
	int64_t size1;
	uint64_t fsize;

    FUNC_ENTER_NOAPI_NOINIT

    printf("--- H5FD_safe_open(%s)\n", name);

    /* Sanity check on file offsets */
    HDcompile_assert(sizeof(HDoff_t) >= sizeof(size_t));

    /* Check arguments */
    if(!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name")
    if(0 == maxaddr || HADDR_UNDEF == maxaddr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr")
    if(ADDR_OVERFLOW(maxaddr))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "bogus maxaddr")

    /* Build the open flags */
    o_flags = (H5F_ACC_RDWR & flags) ? O_RDWR : O_RDONLY;
    if(H5F_ACC_TRUNC & flags)
        o_flags |= O_TRUNC;
    if(H5F_ACC_CREAT & flags)
        o_flags |= O_CREAT;
    if(H5F_ACC_EXCL & flags)
        o_flags |= O_EXCL;

    /* Open the file */
    if((fd = HDopen(name, o_flags, 0666)) < 0) {
        int myerrno = errno;
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to open file: name = '%s', errno = %d, error message = '%s', flags = %x, o_flags = %x", name, myerrno, HDstrerror(myerrno), flags, (unsigned)o_flags);
    } /* end if */



    if(HDfstat(fd, &sb) < 0)
        HSYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, NULL, "unable to fstat file")

    /* Create the new file struct */
    if(NULL == (file = H5FL_CALLOC(H5FD_safe_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate file struct")

    printf("Create file handler %p\n", file);

    file->fd = fd;

    if(H5F_ACC_CREAT & flags) {
    	/** create the log file **/
    	H5FD_safe_file_header_t hdr;
    	ssize_t w;

    	memset(&hdr, 0, sizeof(H5FD_safe_file_header_t));

    	hdr.signature = 0x0a1a0a0d46444890; /* temporary signature \212 H D F \r \n \032 \n */
    	hdr.file_id = (uint64_t)rand();
    	hdr.version = 0;

    	HDftruncate(fd, 0);
    	HDlseek(fd, 0, SEEK_SET);

    	file->map = NULL;
    	file->free = NULL;
    	file->used = NULL;
    	file->prot = NULL;

    	/* set protected data */
    	file->prot = H5FD_safe_alloc_list_add(file->prot, H5FD_safe_alloc_list_new_node(0, sizeof(H5FD_safe_file_header_t)));

    	/* set free space */
    	file->free = H5FD_safe_negative_mask(file->prot);

    	hdr.log0.check_sum = 0;
    	hdr.log0.file_map_offset = 0;
    	//hdr.log0.log_begin_offset = file->log_handler.current_file_offset;
    	hdr.log0.magic_id = (uint64_t)rand();
    	hdr.log0.old_magic_id = 0;

    	file->file_id = hdr.file_id;

    	w = HDwrite(fd, &hdr, sizeof(H5FD_safe_file_header_t));
    	if(w != (ssize_t)sizeof(H5FD_safe_file_header_t))
    		HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to create file: name = '%s'", name);

    } else {
    	H5FD_safe_file_header_t hdr;
    	int valid_log0 = -1;
    	int valid_log1 = -1;
    	ssize_t w;

    	/* TODO: read file header and logs */

    	HDlseek(fd, 0, SEEK_SET);
    	w = HDread(fd, &hdr, sizeof(H5FD_safe_file_header_t));
    	if(w != (ssize_t)sizeof(H5FD_safe_file_header_t))
    		HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to read file: name = '%s'", name);

    	if(hdr.signature != 0x0a1a0a0d46444890) {
    		HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "invalid file signature: name = '%s', signature = %016x", name, hdr.signature);
    	} else {
    		printf("File signature is valid\n");
    	}

    	if(H5FD_safe_load_log(file, &hdr.log0, &map0, &size0) < 0) {
    		valid_log0 = -1;
    	} else {
    		valid_log0 = 0;
    	}

    	if(H5FD_safe_load_log(file, &hdr.log1, &map1, &size1) < 0) {
    		valid_log1 = -1;
    	} else {
    		valid_log1 = 0;
    	}

    	if(valid_log0 < 0 && valid_log1 < 0) {
    		HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "very bad news!!! you file is corrupted.");
    	} else if (valid_log0 == 0 && valid_log1 == 0) {
    		printf("found 2 valid log, keep newer\n");

    		if(hdr.log0.old_magic_id == hdr.log1.magic_id && !(hdr.log0.old_magic_id == hdr.log1.magic_id)) {
    			printf("selecting log 0\n");
    			/** keep log0 **/
    	    	file->file_id = hdr.file_id;
    			file->selected_log = 0;

    			H5FD_safe_log_select(file, &hdr.log0, map0, size0);

    		} else if(hdr.log1.old_magic_id == hdr.log0.magic_id && !(hdr.log0.old_magic_id == hdr.log1.magic_id)) {
    			printf("selecting log 1\n");
    			/** keep log1 **/
    	    	file->file_id = hdr.file_id;
    			file->selected_log = 1;
    			H5FD_safe_log_select(file, &hdr.log1, map1, size1);
    		} else {
    			/** TODO: compute file size for both, keep largest or random select one **/
        		if(H5FD_safe_vfm_file_size(map0) > H5FD_safe_vfm_file_size(map1)) {
        			printf("selecting log 0\n");
        			/** keep log0 **/
        	    	file->file_id = hdr.file_id;
        			file->selected_log = 0;

        			H5FD_safe_log_select(file, &hdr.log0, map0, size0);
        		} else {
        			printf("selecting log 1\n");
        			/** keep log1 **/
        	    	file->file_id = hdr.file_id;
        			file->selected_log = 1;
        			H5FD_safe_log_select(file, &hdr.log1, map1, size1);
        		}
    		}

    	} else if (valid_log0 == 0 && valid_log1 < 0) {
			printf("selecting log 0\n");
			/** keep log0 **/
	    	file->file_id = hdr.file_id;
			file->selected_log = 0;

			H5FD_safe_log_select(file, &hdr.log0, map0, size0);
    	} else if (valid_log0 < 0 && valid_log1 == 0) {
			printf("selecting log 1\n");
			/** keep log1 **/
	    	file->file_id = hdr.file_id;
			file->selected_log = 1;
			H5FD_safe_log_select(file, &hdr.log1, map1, size1);
    	}


    }

    file->fd = fd;
    file->old_real_file_size = H5FD_safe_vfm_file_real_size(file->map);

    /** get virtual file size **/
    fsize = H5FD_safe_vfm_file_size(file->map);
    H5_ASSIGN_OVERFLOW(file->eof, fsize, h5_stat_size_t, haddr_t);
    file->pos = HADDR_UNDEF;
    file->op = OP_UNKNOWN;
#ifdef H5_HAVE_WIN32_API
    file->hFile = (HANDLE)_get_osfhandle(fd);
    if(INVALID_HANDLE_VALUE == file->hFile)
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to get Windows file handle")

    if(!GetFileInformationByHandle((HANDLE)file->hFile, &fileinfo))
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to get Windows file information")

    file->nFileIndexHigh = fileinfo.nFileIndexHigh;
    file->nFileIndexLow = fileinfo.nFileIndexLow;
    file->dwVolumeSerialNumber = fileinfo.dwVolumeSerialNumber;
#else /* H5_HAVE_WIN32_API */
    file->device = sb.st_dev;
#ifdef H5_VMS
    file->inode[0] = sb.st_ino[0];
    file->inode[1] = sb.st_ino[1];
    file->inode[2] = sb.st_ino[2];
#else /* H5_VMS */
    file->inode = sb.st_ino;
#endif /* H5_VMS */
#endif /* H5_HAVE_WIN32_API */

    /* Retain a copy of the name used to open the file, for possible error reporting */
    HDstrncpy(file->filename, name, sizeof(file->filename));
    file->filename[sizeof(file->filename) - 1] = '\0';

    /* Check for non-default FAPL */
    if(H5P_FILE_ACCESS_DEFAULT != fapl_id) {
        H5P_genplist_t      *plist;      /* Property list pointer */

        /* Get the FAPL */
        if(NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
            HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, NULL, "not a file access property list")

        /* This step is for h5repart tool only. If user wants to change file driver from
         * family to sec2 while using h5repart, this private property should be set so that
         * in the later step, the library can ignore the family driver information saved
         * in the superblock.
         */
        if(H5P_exist_plist(plist, H5F_ACS_FAMILY_TO_SEC2_NAME) > 0)
            if(H5P_get(plist, H5F_ACS_FAMILY_TO_SEC2_NAME, &file->fam_to_safe) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get property of changing family to sec2")
    } /* end if */

    /* Set return value */
    ret_value = (H5FD_t*)file;

done:
    if(NULL == ret_value) {
        if(fd >= 0)
            HDclose(fd);
        if(file)
            file = H5FL_FREE(H5FD_safe_t, file);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_safe_open() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_close
 *
 * Purpose:     Closes an HDF5 file.
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL, file not closed.
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_safe_close(H5FD_t *_file)
{
    H5FD_safe_t *file = (H5FD_safe_t *)_file;
    herr_t      ret_value = SUCCEED;                /* Return value */
    //H5FD_safe_log_msg_t hdr;

    H5FD_safe_file_header_t hdr;
    H5FD_safe_log_header_t * log;
    H5FD_safe_log_header_t * log_old;
    char * map;
    int64_t size;
    int64_t offset;
    uint64_t count;
    uint64_t current_real_size;

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity check */
    HDassert(file);

    printf("H5FD_safe_close(%p, %s)\n", file, file->filename);

    /* write log on unprotected one */
    if(file->selected_log == 0) {
    	log = &hdr.log1;
    	log_old = &hdr.log0;
    	printf("write log1\n");
    } else {
    	log = &hdr.log0;
    	log_old = &hdr.log1;
    	printf("write log0\n");
    }

    HDlseek(file->fd, 0, SEEK_SET);
    HDread(file->fd, &hdr, sizeof(H5FD_safe_file_header_t));

    log->magic_id = (uint64_t)rand();
    log->old_magic_id = log_old->magic_id;
    log->check_sum = 0;

    if(log->magic_id == log->old_magic_id)
    	log->magic_id += 1;


    //H5FD_safe_vfm_print(file->map);
    H5FD_safe_vfm_serialize(file->map, hdr.file_id, log->magic_id, &map, &count, &size);
    log->file_map_fragment_count = count;

    offset = H5FD_safe_alloc(file, size);
    log->file_map_offset = (uint64_t)offset;
    HDlseek(file->fd, offset, SEEK_SET);
    HDwrite(file->fd, map, size);

    /* compute the current size */
    current_real_size = H5FD_safe_vfm_file_real_size(file->map);
    if(current_real_size < sizeof(H5FD_safe_file_header_t)) {
    	current_real_size = sizeof(H5FD_safe_file_header_t);
    }

    if(current_real_size < offset+size) {
    	current_real_size = offset+size;
    }

    if(current_real_size < (log_old->file_map_offset + (sizeof(H5FD_safe_vfm_serialize_node_t) * log_old->file_map_fragment_count + sizeof(uint64_t)))) {
    	current_real_size = (log_old->file_map_offset + (sizeof(H5FD_safe_vfm_serialize_node_t) * log_old->file_map_fragment_count + sizeof(uint64_t)));
    }

    HDftruncate(file->fd, current_real_size);
    fsync(file->fd);
    log->check_sum = H5_checksum_fletcher32(log, sizeof(H5FD_safe_log_header_t));
    HDlseek(file->fd, 0, SEEK_SET);
    HDwrite(file->fd, &hdr, sizeof(H5FD_safe_file_header_t));
    fsync(file->fd);


//    printf("H5FD_safe_close(%p, %s)\n", file, file->filename);
//
//    hdr.type = H5FD_SAFE_MSG_CLOSE_FILE;
//    hdr.m_close.size = 0;
//
//    /** ensure log are closed and written on disk **/
//    if(HDwrite(file->log_fd, &hdr, sizeof(H5FD_safe_log_msg_t)) < 0) {
//    	HSYS_GOTO_ERROR(H5E_IO, H5E_CANTCLOSEFILE, FAIL, "unable to write log file")
//    }
//    fsync(file->fd);

//    /** replay log to write on file **/
//	if(H5FD_safe_replay_log(file->filename, file->log_filename) < 0) {
//		printf("FAIL to read log\n");
//	} else {
//	    HDunlink(file->log_filename);
//	}

    /* Close the underlying file */
    if(HDclose(file->fd) < 0)
        HSYS_GOTO_ERROR(H5E_IO, H5E_CANTCLOSEFILE, FAIL, "unable to close file")

    /** remove temporary file **/
    printf("--- H5FD_safe_close(%p, %s)\n", file, file->filename);


    /* Release the file info */
    file = H5FL_FREE(H5FD_safe_t, file);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_safe_close() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_cmp
 *
 * Purpose:     Compares two files belonging to this driver using an
 *              arbitrary (but consistent) ordering.
 *
 * Return:      Success:    A value like strcmp()
 *              Failure:    never fails (arguments were checked by the
 *                          caller).
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD_safe_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_safe_t   *f1 = (const H5FD_safe_t *)_f1;
    const H5FD_safe_t   *f2 = (const H5FD_safe_t *)_f2;
    int ret_value = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

#ifdef H5_HAVE_WIN32_API
    if(f1->dwVolumeSerialNumber < f2->dwVolumeSerialNumber) HGOTO_DONE(-1)
    if(f1->dwVolumeSerialNumber > f2->dwVolumeSerialNumber) HGOTO_DONE(1)

    if(f1->nFileIndexHigh < f2->nFileIndexHigh) HGOTO_DONE(-1)
    if(f1->nFileIndexHigh > f2->nFileIndexHigh) HGOTO_DONE(1)

    if(f1->nFileIndexLow < f2->nFileIndexLow) HGOTO_DONE(-1)
    if(f1->nFileIndexLow > f2->nFileIndexLow) HGOTO_DONE(1)
#else /* H5_HAVE_WIN32_API */
#ifdef H5_DEV_T_IS_SCALAR
    if(f1->device < f2->device) HGOTO_DONE(-1)
    if(f1->device > f2->device) HGOTO_DONE(1)
#else /* H5_DEV_T_IS_SCALAR */
    /* If dev_t isn't a scalar value on this system, just use memcmp to
     * determine if the values are the same or not.  The actual return value
     * shouldn't really matter...
     */
    if(HDmemcmp(&(f1->device),&(f2->device),sizeof(dev_t)) < 0) HGOTO_DONE(-1)
    if(HDmemcmp(&(f1->device),&(f2->device),sizeof(dev_t)) > 0) HGOTO_DONE(1)
#endif /* H5_DEV_T_IS_SCALAR */
#ifdef H5_VMS
    if(HDmemcmp(&(f1->inode), &(f2->inode), 3 * sizeof(ino_t)) < 0) HGOTO_DONE(-1)
    if(HDmemcmp(&(f1->inode), &(f2->inode), 3 * sizeof(ino_t)) > 0) HGOTO_DONE(1)
#else /* H5_VMS */
    if(f1->inode < f2->inode) HGOTO_DONE(-1)
    if(f1->inode > f2->inode) HGOTO_DONE(1)
#endif /* H5_VMS */
#endif /* H5_HAVE_WIN32_API */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_safe_cmp() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:      SUCCEED (Can't fail)
 *
 * Programmer:  Quincey Koziol
 *              Friday, August 25, 2000
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_safe_query(const H5FD_t *_file, unsigned long *flags /* out */)
{
    const H5FD_safe_t	*file = (const H5FD_safe_t *)_file;    /* sec2 VFD info */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Set the VFL feature flags that this driver supports */
    if(flags) {
        *flags = 0;
        *flags |= H5FD_FEAT_AGGREGATE_METADATA;     /* OK to aggregate metadata allocations                             */
        *flags |= H5FD_FEAT_ACCUMULATE_METADATA;    /* OK to accumulate metadata for faster writes                      */
        *flags |= H5FD_FEAT_DATA_SIEVE;             /* OK to perform data sieving for faster raw data reads & writes    */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA;    /* OK to aggregate "small" raw data allocations                     */
        *flags |= H5FD_FEAT_POSIX_COMPAT_HANDLE;    /* VFD handle is POSIX I/O call compatible                          */

        /* Check for flags that are set by h5repart */
        if(file && file->fam_to_safe)
            *flags |= H5FD_FEAT_IGNORE_DRVRINFO; /* Ignore the driver info when file is opened (which eliminates it) */
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD_safe_query() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_get_eoa
 *
 * Purpose:     Gets the end-of-address marker for the file. The EOA marker
 *              is the first address past the last byte allocated in the
 *              format address space.
 *
 * Return:      The end-of-address marker.
 *
 * Programmer:  Robb Matzke
 *              Monday, August  2, 1999
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_safe_get_eoa(const H5FD_t *_file, H5FD_mem_t UNUSED type)
{
    const H5FD_safe_t	*file = (const H5FD_safe_t *)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(file->eoa)
} /* end H5FD_safe_get_eoa() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_set_eoa
 *
 * Purpose:     Set the end-of-address marker for the file. This function is
 *              called shortly after an existing HDF5 file is opened in order
 *              to tell the driver where the end of the HDF5 data is located.
 *
 * Return:      SUCCEED (Can't fail)
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_safe_set_eoa(H5FD_t *_file, H5FD_mem_t UNUSED type, haddr_t addr)
{
    H5FD_safe_t	*file = (H5FD_safe_t *)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    file->eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD_safe_set_eoa() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_get_eof
 *
 * Purpose:     Returns the end-of-file marker, which is the greater of
 *              either the filesystem end-of-file or the HDF5 end-of-address
 *              markers.
 *
 * Return:      End of file address, the first address past the end of the 
 *              "file", either the filesystem file or the HDF5 file.
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_safe_get_eof(const H5FD_t *_file)
{
    const H5FD_safe_t   *file = (const H5FD_safe_t *)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(MAX(file->eof, file->eoa))
} /* end H5FD_safe_get_eof() */


/*-------------------------------------------------------------------------
 * Function:       H5FD_safe_get_handle
 *
 * Purpose:        Returns the file handle of sec2 file driver.
 *
 * Returns:        SUCCEED/FAIL
 *
 * Programmer:     Raymond Lu
 *                 Sept. 16, 2002
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_safe_get_handle(H5FD_t *_file, hid_t UNUSED fapl, void **file_handle)
{
    H5FD_safe_t         *file = (H5FD_safe_t *)_file;
    herr_t              ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    if(!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file handle not valid")

    *file_handle = &(file->fd);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_safe_get_handle() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_read
 *
 * Purpose:     Reads SIZE bytes of data from FILE beginning at address ADDR
 *              into buffer BUF according to data transfer properties in
 *              DXPL_ID.
 *
 * Return:      Success:    SUCCEED. Result is stored in caller-supplied
 *                          buffer BUF.
 *              Failure:    FAIL, Contents of buffer BUF are undefined.
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_safe_read(H5FD_t *_file, H5FD_mem_t UNUSED type, hid_t UNUSED dxpl_id,
    haddr_t addr, size_t size, void *buf /*out*/)
{
    H5FD_safe_t     *file       = (H5FD_safe_t *)_file;
    herr_t          ret_value   = SUCCEED;                  /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    printf("H5FD_safe_read(%ld, %ld)\n", addr, size);

    HDassert(file && file->pub.cls);
    HDassert(buf);

    /* Check for overflow conditions */
    if(!H5F_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr)
    if(REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu", (unsigned long long)addr)
    if((addr + size) > file->eoa)
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu, size=%lu, eoa=%llu", 
                    (unsigned long long)addr, size, (unsigned long long)file->eoa)

    /* Seek to the correct location */
    if(addr != file->pos || OP_READ != file->op) {
        if(HDlseek(file->fd, (HDoff_t)addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")
    } /* end if */

    /* Read data, being careful of interrupted system calls, partial results,
     * and the end of the file.
     */
    while(size > 0) {

        h5_posix_io_t       bytes_in        = 0;    /* # of bytes to read       */
        h5_posix_io_ret_t   bytes_read      = -1;   /* # of bytes actually read */ 

        /* Trying to read more bytes than the return type can handle is
         * undefined behavior in POSIX.
         */
        if(size > H5_POSIX_MAX_IO_BYTES)
            bytes_in = H5_POSIX_MAX_IO_BYTES;
        else
            bytes_in = (h5_posix_io_t)size;

        do {
            bytes_read = H5FD_safe_virtual_read(file, buf, addr, bytes_in);
        } while(-1 == bytes_read && EINTR == errno);
        
        if(-1 == bytes_read) { /* error */
            int myerrno = errno;
            time_t mytime = HDtime(NULL);
            HDoff_t myoffset = HDlseek(file->fd, (HDoff_t)0, SEEK_CUR);

            HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file read failed: time = %s, filename = '%s', file descriptor = %d, errno = %d, error message = '%s', buf = %p, total read size = %llu, bytes this sub-read = %llu, bytes actually read = %llu, offset = %llu", HDctime(&mytime), file->filename, file->fd, myerrno, HDstrerror(myerrno), buf, (unsigned long long)size, (unsigned long long)bytes_in, (unsigned long long)bytes_read, (unsigned long long)myoffset);
        } /* end if */
        
        if(0 == bytes_read) {
            /* end of file but not end of format address space */
            HDmemset(buf, 0, size);
            break;
        } /* end if */
        
        HDassert(bytes_read >= 0);
        HDassert((size_t)bytes_read <= size);
        
        size -= (size_t)bytes_read;
        addr += (haddr_t)bytes_read;
        buf = (char *)buf + bytes_read;
    } /* end while */

    /* Update current position */
    file->pos = addr;
    file->op = OP_READ;

done:
    if(ret_value < 0) {
        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op = OP_UNKNOWN;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_safe_read() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_write
 *
 * Purpose:     Writes SIZE bytes of data to FILE beginning at address ADDR
 *              from buffer BUF according to data transfer properties in
 *              DXPL_ID.
 *
 * Return:      SUCCEED/FAIL
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_safe_write(H5FD_t *_file, H5FD_mem_t UNUSED type, hid_t UNUSED dxpl_id,
                haddr_t addr, size_t size, const void *buf)
{
    H5FD_safe_t     *file       = (H5FD_safe_t *)_file;
    herr_t          ret_value   = SUCCEED;                  /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    printf("H5FD_safe_write(%ld, %ld)\n", addr, size);

    HDassert(file && file->pub.cls);
    HDassert(buf);

    /* Check for overflow conditions */
    if(!H5F_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr)
    if(REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu, size = %llu", (unsigned long long)addr, (unsigned long long)size)
    if((addr + size) > file->eoa)
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu, size = %llu, eoa = %llu", (unsigned long long)addr, (unsigned long long)size, (unsigned long long)file->eoa)

    /* Seek to the correct location */
    if(addr != file->pos || OP_WRITE != file->op) {
        if(HDlseek(file->fd, (HDoff_t)addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")
    } /* end if */

    /* Write the data, being careful of interrupted system calls and partial
     * results
     */
    while(size > 0) {

        h5_posix_io_t       bytes_in        = 0;    /* # of bytes to write  */
        h5_posix_io_ret_t   bytes_wrote     = -1;   /* # of bytes written   */ 

        /* Trying to write more bytes than the return type can handle is
         * undefined behavior in POSIX.
         */
        if(size > H5_POSIX_MAX_IO_BYTES)
            bytes_in = H5_POSIX_MAX_IO_BYTES;
        else
            bytes_in = (h5_posix_io_t)size;

        do {
        	bytes_wrote = H5FD_safe_virtual_write(file, buf, addr, bytes_in);

        } while(-1 == bytes_wrote && EINTR == errno);
        
        if(-1 == bytes_wrote) { /* error */
            int myerrno = errno;
            time_t mytime = HDtime(NULL);
            HDoff_t myoffset = HDlseek(file->fd, (HDoff_t)0, SEEK_CUR);

            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed: time = %s, filename = '%s', file descriptor = %d, errno = %d, error message = '%s', buf = %p, total write size = %llu, bytes this sub-write = %llu, bytes actually written = %llu, offset = %llu", HDctime(&mytime), file->filename, file->fd, myerrno, HDstrerror(myerrno), buf, (unsigned long long)size, (unsigned long long)bytes_in, (unsigned long long)bytes_wrote, (unsigned long long)myoffset);
        } /* end if */
        
        HDassert(bytes_wrote > 0);
        HDassert((size_t)bytes_wrote <= size);

        size -= (size_t)bytes_wrote;
        addr += (haddr_t)bytes_wrote;
        buf = (const char *)buf + bytes_wrote;
    } /* end while */

    /* Update current position and eof */
    file->pos = addr;
    file->op = OP_WRITE;
    if(file->pos > file->eof)
        file->eof = file->pos;

done:
    if(ret_value < 0) {
        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op = OP_UNKNOWN;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_safe_write() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_safe_truncate
 *
 * Purpose:     Makes sure that the true file size is the same (or larger)
 *              than the end-of-address.
 *
 * Return:      SUCCEED/FAIL
 *
 * Programmer:  Robb Matzke
 *              Wednesday, August  4, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_safe_truncate(H5FD_t *_file, hid_t UNUSED dxpl_id, hbool_t UNUSED closing)
{
    H5FD_safe_t *file = (H5FD_safe_t *)_file;
    herr_t ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(file);

    printf("H5FD_safe_truncate(%ld)\n", file->eoa);

    /* Extend the file to make sure it's large enough */
    if(!H5F_addr_eq(file->eoa, file->eof)) {
#ifdef H5_HAVE_WIN32_API
        LARGE_INTEGER   li;         /* 64-bit (union) integer for SetFilePointer() call */
        DWORD           dwPtrLow;   /* Low-order pointer bits from SetFilePointer()
                                     * Only used as an error code here.
                                     */
        DWORD           dwError;    /* DWORD error code from GetLastError() */
        BOOL            bError;     /* Boolean error flag */

        /* Windows uses this odd QuadPart union for 32/64-bit portability */
        li.QuadPart = (__int64)file->eoa;

        /* Extend the file to make sure it's large enough.
         *
         * Since INVALID_SET_FILE_POINTER can technically be a valid return value
         * from SetFilePointer(), we also need to check GetLastError().
         */
        dwPtrLow = SetFilePointer(file->hFile, li.LowPart, &li.HighPart, FILE_BEGIN);
        if(INVALID_SET_FILE_POINTER == dwPtrLow) {
            dwError = GetLastError();
            if(dwError != NO_ERROR )
                HGOTO_ERROR(H5E_FILE, H5E_FILEOPEN, FAIL, "unable to set file pointer")
        }

        bError = SetEndOfFile(file->hFile);
        if(0 == bError)
            HGOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly")
#else /* H5_HAVE_WIN32_API */
#ifdef H5_VMS
        /* Reset seek offset to the beginning of the file, so that the file isn't
         * re-extended later.  This may happen on Open VMS. */
        if(-1 == HDlseek(file->fd, (HDoff_t)0, SEEK_SET))
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")
#endif
        if(-1 == H5FD_safe_virtual_truncate(file, (size_t)file->eoa))
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly")
#endif /* H5_HAVE_WIN32_API */

        /* Update the eof value */
        file->eof = file->eoa;

        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op = OP_UNKNOWN;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_safe_truncate() */
