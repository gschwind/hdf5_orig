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
#define H5_INTERFACE_INIT_FUNC  H5FD_sid_init_interface


#include "H5private.h"      /* Generic Functions        */
#include "H5Eprivate.h"     /* Error handling           */
#include "H5Fprivate.h"     /* File access              */
#include "H5FDprivate.h"    /* File drivers             */
#include "H5FDsid.h"        /* sid file driver         */
#include "H5FLprivate.h"    /* Free Lists               */
#include "H5Iprivate.h"     /* IDs                      */
#include "H5MMprivate.h"    /* Memory management        */
#include "H5Pprivate.h"     /* Property lists           */


/* enum to use in virtual file map to specify the location of data */
typedef enum H5FD_sid_source_id_t {
	H5FD_SID_SRC_EMPTY, /* data are not alocated */
	H5FD_SID_SRC_FILE,  /* data are located in original file */
	H5FD_SID_SRC_LOG    /* data are located in journal file */
} H5FD_sid_source_id_t;


/**
 * simple sorted list of file virtual file map.
 *
 * This map allow to create a virtual continuous file that is stored several files. in this case
 * in journal file and/or original file and/or empty area (area that is not already written).
 *
 * By usage this map cannot have node with size == 0.
 *
 */
typedef struct H5FD_sid_vfm_t {
	int64_t voffset;			/* offset in virtual file */
	int64_t roffset;			/* offset in the current real file */
	int64_t size;				/* size of data */
	int64_t file_source;		/* file source, is it from log ? */
	struct H5FD_sid_vfm_t * next;
	struct H5FD_sid_vfm_t * prev;
} H5FD_sid_virtual_file_map_t;

/* short name for the virtual_file_map */
typedef H5FD_sid_virtual_file_map_t H5FD_sid_vfm_t;

/* The driver identification number, initialized at runtime */
static hid_t H5FD_SID_g = 0;

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
typedef struct H5FD_sid_t {
    H5FD_t          pub;    /* public stuff, must be first      */
    int             fd;     /* the filesystem file descriptor   */
    haddr_t         eoa;    /* end of allocated region          */
    haddr_t         eof;    /* end of file; current file size   */
    haddr_t         pos;    /* current file I/O position        */
    H5FD_file_op_t  op;     /* last operation                   */
    char            filename[H5FD_MAX_FILENAME_LEN];    /* Copy of file name from open operation */

    int             log_fd; /* handle the journal file */
    char            log_filename[H5FD_MAX_FILENAME_LEN]; /* Copy of file name from open operation */

    H5FD_sid_vfm_t * map;   /* store the map between the current file and log file */

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
    hbool_t         fam_to_sid;
} H5FD_sid_t;


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
typedef struct H5FD_sid_log_msg_any_t {
	int64_t type;
	int64_t size;
} H5FD_sid_log_msg_any_t;

/* write message store write operation, data that must be written and there location of this data */
typedef struct H5FD_sid_log_msg_write_t {
	int64_t type;
	int64_t size;
	int64_t voffset;
} H5FD_sid_log_msg_write_t;

/* truncate message store truncate operation */
typedef struct H5FD_sid_log_msg_truncate_t {
	int64_t type;
	int64_t size;
	int64_t truncate_to;
} H5FD_sid_log_msg_truncate_t;

/* close operation store close operation */
typedef struct H5FD_sid_log_msg_close_t {
	int64_t type;
	int64_t size;
} H5FD_sid_log_msg_close_t;

/* union of all messages type, this define the size of message header */
typedef union H5FD_sid_log_msg_t {
	int64_t type;
	H5FD_sid_log_msg_any_t m_any;
	H5FD_sid_log_msg_write_t m_write;
	H5FD_sid_log_msg_truncate_t m_trunk;
	H5FD_sid_log_msg_close_t m_close;
} H5FD_sid_log_msg_t;


/* list of message type and forseen message type */
typedef enum H5FD_sid_mesage_type_t {
	H5FD_SID_MSG_NONE = 0,					/* NO USED */
	H5FD_SID_MSG_WRITE,					/* write operation */
	H5FD_SID_MSG_WRITE_IN_FILE,			/* write in file, not used yet, but it is possible to
											   write new data directly in file without corrupting
											   the hdf5, when this data are wrote after the end of
											   file when it was opened */
	H5FD_SID_MSG_WRITE_IN_FILE_DONE,    /* not used yet see previous comment */
	H5FD_SID_MSG_CLOSE_FILE,			/* mark file close to validate log */
	H5FD_SID_MSG_TRUNCATE,				/* truncate masage */
	H5FD_SID_MSG_LAST
} H5FD_sid_mesage_type_t;

/* char for debuging purpose */
static char const * const s_mesage_type[] = {
		"H5FD_SID_MSG_NONE",
		"H5FD_SID_MSG_WRITE",
		"H5FD_SID_MSG_WRITE_IN_FILE",
		"H5FD_SID_MSG_WRITE_IN_FILE_DONE",
		"H5FD_SID_MSG_CLOSE_FILE",
		"H5FD_SID_MSG_TRUNCATE",
		"H5FD_SID_MSG_LAST",
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
static H5FD_t *H5FD_sid_open(const char *name, unsigned flags, hid_t fapl_id,
            haddr_t maxaddr);
static herr_t H5FD_sid_close(H5FD_t *_file);
static int H5FD_sid_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t H5FD_sid_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD_sid_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t H5FD_sid_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD_sid_get_eof(const H5FD_t *_file);
static herr_t  H5FD_sid_get_handle(H5FD_t *_file, hid_t fapl, void** file_handle);
static herr_t H5FD_sid_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr,
            size_t size, void *buf);
static herr_t H5FD_sid_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr,
            size_t size, const void *buf);
static herr_t H5FD_sid_truncate(H5FD_t *_file, hid_t dxpl_id, hbool_t closing);


static H5FD_sid_vfm_t * H5FD_sid_vfm_new(int64_t size);
static void H5FD_sid_vfm_delete(H5FD_sid_vfm_t * ths);
static H5FD_sid_vfm_t * H5FD_sid_vfm_find_node(H5FD_sid_vfm_t * ths, int64_t offset);
static H5FD_sid_vfm_t * H5FD_sid_vfm_split(H5FD_sid_vfm_t * ths, int64_t offset);
static H5FD_sid_vfm_t * H5FD_sid_vfm_insert(H5FD_sid_vfm_t * ths, int64_t voffset, int64_t roffset, int64_t size);
static H5FD_sid_vfm_t * H5FD_sid_vfm_truncate(H5FD_sid_vfm_t * ths, size_t size);
static H5FD_sid_vfm_t * H5FD_sid_vfm_grow(H5FD_sid_vfm_t * ths, size_t size);
static H5FD_sid_vfm_t * H5FD_sid_vfm_remove(H5FD_sid_vfm_t * ths, H5FD_sid_vfm_t * first, H5FD_sid_vfm_t * last);
static void H5FD_sid_vfm_print(H5FD_sid_vfm_t * ths);
static H5FD_sid_vfm_t * H5FD_sid_vfm_extract(H5FD_sid_vfm_t * ths, int64_t offset, int64_t size);


static herr_t H5FD_sid_replay_log(char const * filename, char const * log_filename);

static herr_t H5FD_sid_virtual_truncate(H5FD_sid_t * file, size_t size);

static const H5FD_class_t H5FD_sid_g = {
    "sid",                     /* name                 */
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
    H5FD_sid_open,             /* open                 */
    H5FD_sid_close,            /* close                */
    H5FD_sid_cmp,              /* cmp                  */
    H5FD_sid_query,            /* query                */
    NULL,                       /* get_type_map         */
    NULL,                       /* alloc                */
    NULL,                       /* free                 */
    H5FD_sid_get_eoa,          /* get_eoa              */
    H5FD_sid_set_eoa,          /* set_eoa              */
    H5FD_sid_get_eof,          /* get_eof              */
    H5FD_sid_get_handle,       /* get_handle           */
    H5FD_sid_read,             /* read                 */
    H5FD_sid_write,            /* write                */
    NULL,                       /* flush                */
    H5FD_sid_truncate,         /* truncate             */
    NULL,                       /* lock                 */
    NULL,                       /* unlock               */
    H5FD_FLMAP_DICHOTOMY        /* fl_map               */
};

/* Declare a free list to manage the H5FD_sid_t struct */
H5FL_DEFINE_STATIC(H5FD_sid_t);

static H5FD_sid_vfm_t *
H5FD_sid_vfm_extract(H5FD_sid_vfm_t * ths, int64_t offset, int64_t size) {
	H5FD_sid_vfm_t * cur_src, * cur_ret;
	//printf("H5FD_sid_vfm_extract(offset = %ld, size = %ld)\n", offset, size);

	cur_src = ths;
	cur_ret = NULL;
	while(cur_src != NULL) {
		int64_t x_start = MAX(offset, cur_src->voffset);
		int64_t x_end = MIN(offset+size, cur_src->voffset + cur_src->size);

		if(x_end - x_start > 0) {
			/* create new node */
			H5FD_sid_vfm_t * x = (H5FD_sid_vfm_t *) malloc(sizeof(H5FD_sid_vfm_t));
			x->file_source = cur_src->file_source;
			x->roffset = cur_src->roffset + x_start - cur_src->voffset;
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


static void
H5FD_sid_vfm_print(H5FD_sid_vfm_t * ths) {
	H5FD_sid_vfm_t * cur = ths;
	return;
	printf("MAP BEGIN\n");
	while(cur != NULL) {
		char const * s = NULL;

		switch(cur->file_source) {
		case H5FD_SID_SRC_FILE:
			s = "FILE";
			break;
		case H5FD_SID_SRC_LOG:
			s = "LOG";
			break;
		case H5FD_SID_SRC_EMPTY:
			s = "EMPTY";
			break;
		default:
			s = "UNKNOW";
			break;
		}

		printf("start = %ld, end = %ld, poffset = %ld, size = %ld, type = %s, cur = %p, next = %p, prev = %p\n",
				cur->voffset, cur->voffset + cur->size, cur->roffset, cur->size, s, cur, cur->next, cur->prev);

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


static H5FD_sid_vfm_t *
H5FD_sid_vfm_new(int64_t size) {
	H5FD_sid_vfm_t * ret_value;
	if(size == 0)
		return NULL;
	ret_value = (H5FD_sid_vfm_t*)malloc(sizeof(H5FD_sid_vfm_t));
	if(ret_value == NULL)
		return NULL;

	if(size == 0)
		return NULL;

	ret_value->voffset = 0;
	ret_value->roffset = 0;
	ret_value->size = size;
	ret_value->file_source = H5FD_SID_SRC_FILE;
	ret_value->next = NULL;
	ret_value->prev = NULL;

	H5FD_sid_vfm_print(ret_value);

	return ret_value;
}

static void
H5FD_sid_vfm_delete(H5FD_sid_vfm_t * ths) {
	H5FD_sid_vfm_remove(ths, ths, NULL);
}

static H5FD_sid_vfm_t *
H5FD_sid_vfm_find_node(H5FD_sid_vfm_t * ths, int64_t offset) {
	H5FD_sid_vfm_t * cur = ths;
	while(cur != NULL) {
		if(cur->voffset <= offset && (cur->voffset + cur->size) > offset)
			return cur;
		cur = cur->next;
	}
	return NULL;
}

static H5FD_sid_vfm_t *
H5FD_sid_vfm_split(H5FD_sid_vfm_t * ths, int64_t offset) {
	H5FD_sid_vfm_t * node = H5FD_sid_vfm_find_node(ths, offset);
	if(node == NULL)
		return ths;

	//printf("voffset = %ld, offset = %ld\n", node->voffset, offset);

	if (node->voffset != offset) {
		H5FD_sid_vfm_t * insert = (H5FD_sid_vfm_t *)malloc(sizeof(H5FD_sid_vfm_t));;
		if(insert == NULL)
			return ths;

		/* create new node */
		insert->voffset = offset;
		insert->roffset = node->roffset + offset - node->voffset;
		insert->size = node->voffset + node->size - offset;
		insert->file_source = node->file_source;
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

static H5FD_sid_vfm_t *
H5FD_sid_vfm_remove(H5FD_sid_vfm_t * ths, H5FD_sid_vfm_t * first, H5FD_sid_vfm_t * last) {
	H5FD_sid_vfm_t * ret_value, * cur;

	printf("H5FD_sid_vfm_remove(%p,%p,%p)\n", ths, first, last);

	H5FD_sid_vfm_print(ths);

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
		H5FD_sid_vfm_t * tmp = cur;
		cur = cur->next;
		free(tmp);
	}

	if(cur == NULL && last != NULL)
		printf("WARNING: inconsistent remove\n");

	//H5FD_sid_vfm_print(ret_value);

	return ret_value;
}

static H5FD_sid_vfm_t *
H5FD_sid_vfm_insert(H5FD_sid_vfm_t * ths, int64_t voffset, int64_t roffset, int64_t size) {

	H5FD_sid_vfm_t * node_first;
	H5FD_sid_vfm_t * node_last;

	printf("H5FD_sid_vfm_insert(voffset=%ld,roffset=%ld,size=%ld)\n", voffset, roffset, size);

	/**
	 * trick to have plain node on voffset and voffset+size;
	 **/
	ths = H5FD_sid_vfm_split(ths, voffset);
	ths = H5FD_sid_vfm_split(ths, voffset + size);

	if(size <= 0)
		return ths;

	node_first = H5FD_sid_vfm_find_node(ths, voffset);
	node_last = H5FD_sid_vfm_find_node(ths, voffset + size);

	H5FD_sid_vfm_print(ths);

	HDassert(node_first != NULL);
	HDassert(node_first->voffset == voffset);
	HDassert(node_last->voffset == voffset + size);

	ths = H5FD_sid_vfm_remove(ths, node_first->next, node_last);

	/** update node_first **/
	node_first->roffset = roffset;
	node_first->file_source = H5FD_SID_SRC_LOG;
	node_first->size = size;

	H5FD_sid_vfm_print(ths);

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
static herr_t
H5FD_sid_replay_log(char const * filename, char const * log_filename) {

	int fd;
	int log_fd;
	h5_stat_t sb;
	H5FD_sid_vfm_t * map;
	H5FD_sid_log_msg_t hdr;
	int64_t max_write_size = 0;
	int valid_log = 0;

	printf("Replay journal for '%s' with '%s'\n", filename, log_filename);

	fd = HDopen(filename, O_RDWR, 0666);
	if(fd < 0) {
		printf("fail to open '%s'\n", filename);
		return -1;
	}

	log_fd = HDopen(log_filename, O_RDONLY, 0444);
	if(log_fd < 0) {
		printf("fail to open '%s'\n", log_filename);
		return -1;
	}

	HDfstat(fd, &sb);
	map = H5FD_sid_vfm_new(sb.st_size);

	HDlseek(log_fd, 0, SEEK_SET);

	/**
	 * Step 1. check if log file is valid.
	 **/
	while(1) {
		if(HDread(log_fd, &hdr, sizeof(H5FD_sid_log_msg_t)) < (ssize_t)sizeof(H5FD_sid_log_msg_t)) {
			printf("fail to read header\n");
			/* invalid log */
			break;
		}

		if(hdr.type == H5FD_SID_MSG_WRITE) {
			if(max_write_size < hdr.m_any.size) {
				max_write_size = hdr.m_any.size;
			}

			printf("header type = %s, size = %d\n", s_mesage_type[hdr.m_write.type], hdr.m_write.size);

		} else if (hdr.type == H5FD_SID_MSG_CLOSE_FILE) {
			printf("header type = %s, size = %d\n", s_mesage_type[hdr.m_any.type], hdr.m_any.size);
			valid_log = 1;
			break;
		} else if (hdr.type == H5FD_SID_MSG_TRUNCATE) {
			printf("header type = %s, size = %d\n", s_mesage_type[hdr.m_any.type], hdr.m_any.size);

			/** continue **/
		} else {
			/** unknown type => invalid log **/
			break;
		}


		/* goto next message */
		HDlseek(log_fd, hdr.m_any.size, SEEK_CUR);

	}

	/**
	 * if log is valid go rebuild virtual file map.
	 **/

	if(valid_log == 1) {

		/** go back to begin **/
		HDlseek(log_fd, 0, SEEK_SET);

		while(1) {
			if(HDread(log_fd, &hdr, sizeof(hdr)) < sizeof(hdr)) {
				/* should not fail ... */
				return -1;
			}

			if(hdr.type == H5FD_SID_MSG_WRITE) {
				map = H5FD_sid_vfm_grow(map, (size_t)(hdr.m_write.voffset + hdr.m_write.size));
				map = H5FD_sid_vfm_insert(map, hdr.m_write.voffset, HDlseek(log_fd, 0, SEEK_CUR), hdr.m_write.size);
			} else if (hdr.type == H5FD_SID_MSG_CLOSE_FILE) {
				break;
			} else if (hdr.type == H5FD_SID_MSG_TRUNCATE) {
				map = H5FD_sid_vfm_truncate(map, (size_t)hdr.m_trunk.truncate_to);
			} else {
				/** unexpected someone are writing log file ? **/
				return -1;
			}

			/* goto next message */
			HDlseek(log_fd, hdr.m_any.size, SEEK_CUR);

		}

		/* replay MAP */
		printf("Replay MAP\n");
		H5FD_sid_vfm_print(map);

		/**
		 * write operation.
		 **/

		{
			H5FD_sid_vfm_t * cur = map;

			/** Allocate the maximum needed write size **/
			unsigned char * buf = (unsigned char *)malloc((size_t)max_write_size);
			if(buf == NULL)
				return -1;


			while(cur != NULL) {
				if(cur->file_source == H5FD_SID_SRC_LOG) {

					/* read from log */
					HDlseek(log_fd, cur->roffset, SEEK_SET);
					if(HDread(log_fd, buf, (size_t)cur->size) < 0)
						return -1;

					/* write to hdf5 file */

					printf("H5FD_sid_write(FILE,%ld,%ld)\n", cur->voffset, cur->size);
					HDlseek(fd, cur->voffset, SEEK_SET);
					if(HDwrite(fd, buf, (size_t)cur->size) < 0)
						return -1;
				}

				cur = cur->next;
			}

			free(buf);
		}

		fsync(fd);
		close(fd);
		close(log_fd);

		H5FD_sid_vfm_delete(map);

		/** now we can trash log file **/
	} else {
		return -1;
	}

	return 0;

}

static ssize_t H5FD_sid_virtual_write(H5FD_sid_t * file, void const * buf, int64_t addr, size_t size) {
	ssize_t ret_value = -1;

	/** write to log file **/
	H5FD_sid_log_msg_t header;

	printf("H5FD_sid_virtual_write(%ld,%ld)\n", addr, size);

	HDassert(file->log_fd != -1);

	H5FD_sid_vfm_print(file->map);

	header.m_write.type = H5FD_SID_MSG_WRITE;
	header.m_write.size = (int64_t)size;
	header.m_write.voffset = addr;

	HDlseek(file->log_fd, 0, SEEK_END);

	/* write header in log */
	ret_value = HDwrite(file->log_fd, &header, sizeof(H5FD_sid_log_msg_t));

	printf("writed Header : %ld %ld\n", sizeof(H5FD_sid_log_msg_t), ret_value);

	if(ret_value < sizeof(header))
		return -1;

	/* grow if needed */
	H5FD_sid_vfm_print(file->map);
	file->map = H5FD_sid_vfm_grow(file->map, (size_t)(header.m_write.voffset + header.m_write.size));
	//printf("Grow:\n");
	H5FD_sid_vfm_print(file->map);
	file->map = H5FD_sid_vfm_insert(file->map, header.m_write.voffset, (int64_t)HDlseek(file->log_fd, 0, SEEK_CUR), (int64_t)size);

	printf("seek = %d\n", (int64_t)HDlseek(file->log_fd, 0, SEEK_CUR));

	H5FD_sid_vfm_print(file->map);

	/* write data in file */
	ret_value = HDwrite(file->log_fd, buf, size);
	printf("writed data : %d %d\n", size, ret_value);
	return ret_value;

}

static ssize_t
H5FD_sid_virtual_read(H5FD_sid_t * file, void * buf, int64_t addr, size_t const size) {

	ssize_t ret_value = 0;
	unsigned char * xbuf;
	int64_t voffset;
	H5FD_sid_vfm_t * cur;
	H5FD_sid_vfm_t * ext;

	voffset = addr;

	/* extract current read from map */
	ext = H5FD_sid_vfm_extract(file->map, voffset, (int64_t)size);
	//H5FD_sid_vfm_print(file->map);
	printf("Extracted:\n");
	H5FD_sid_vfm_print(ext);

	cur = ext;
	xbuf = (unsigned char *)buf;
	while(cur != NULL) {
		if(cur->file_source == H5FD_SID_SRC_FILE) {
			int64_t read_size = 0;

			if(HDlseek(file->fd, cur->voffset, SEEK_SET) < 0)
				return -1;

			printf("H5FD_sid_virtual_read(FILE,%ld,%ld)\n", cur->voffset, cur->size);
			if((read_size = (int64_t)HDread(file->fd, xbuf, cur->size)) < 0)
				return -1;

			if(read_size < cur->size) {
				return ret_value + read_size;
			}

			xbuf += read_size;
			ret_value += read_size;

		} else if (cur->file_source == H5FD_SID_SRC_LOG) {
			int64_t read_size = 0;

			if(HDlseek(file->log_fd, cur->roffset, SEEK_SET) < 0)
				return -1;

			printf("H5FD_sid_virtual_read(LOG,%ld,%ld)\n", cur->roffset, cur->size);
			if((read_size = (int64_t)HDread(file->log_fd, xbuf, cur->size)) < 0)
				return -1;

			if(read_size < cur->size) {
				return ret_value + read_size;
			}

			xbuf += read_size;
			ret_value += read_size;
		}

		cur = cur->next;
	}

	H5FD_sid_vfm_delete(ext);

	return ret_value;

}

static H5FD_sid_vfm_t *
H5FD_sid_vfm_truncate(H5FD_sid_vfm_t * ths, size_t size) {

	H5FD_sid_vfm_t * node = H5FD_sid_vfm_find_node(ths,
			(int64_t) size);
	if (node != NULL) {
		/** reduce size **/
		ths = H5FD_sid_vfm_split(ths, (int64_t) size);
		node = H5FD_sid_vfm_find_node(ths, (int64_t) size);
		ths = H5FD_sid_vfm_remove(ths, node, NULL);
	} else {
		/** grow size **/
		ths = H5FD_sid_vfm_grow(ths, size);
	}

	return ths;
}

static H5FD_sid_vfm_t *
H5FD_sid_vfm_grow(H5FD_sid_vfm_t * ths, size_t size) {

	if (ths == NULL) {
		/* the lis is empty, grow size */
		ths = (H5FD_sid_vfm_t *) malloc(sizeof(H5FD_sid_vfm_t));
		ths->file_source = H5FD_SID_SRC_EMPTY;
		ths->voffset = 0;
		ths->roffset = 0;
		ths->next = NULL;
		ths->prev = NULL;
		ths->size = (int64_t)size;
	} else {
		H5FD_sid_vfm_t * node;

		/* find last node */
		node = ths;
		while (node->next != NULL) {
			node = node->next;
		}

		if (node->voffset + node->size < (int64_t) size) {
			node->next = (H5FD_sid_vfm_t *) malloc(sizeof(H5FD_sid_vfm_t));
			node->next->file_source = H5FD_SID_SRC_EMPTY;
			node->next->voffset = node->voffset + node->size;
			node->next->roffset = node->voffset + node->size;
			node->next->size = (int64_t) size - node->next->voffset;
			node->next->next = NULL;
			node->next->prev = node;
		}

	}

	return ths;
}

static herr_t
H5FD_sid_virtual_truncate(H5FD_sid_t * file, size_t size) {
	H5FD_sid_log_msg_t msg;

	file->map = H5FD_sid_vfm_truncate(file->map, size);

	msg.m_trunk.type = H5FD_SID_MSG_TRUNCATE;
	msg.m_trunk.size = 0;
	msg.m_trunk.truncate_to = (int64_t)size;

	return (herr_t)HDwrite(file->log_fd, &msg, sizeof(H5FD_sid_log_msg_t));

}



/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_init_interface
 *
 * Purpose:     Initializes any interface-specific data or routines.
 *
 * Return:      Success:    The driver ID for the sec2 driver.
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_sid_init_interface(void)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(H5FD_sid_init())
} /* H5FD_sid_init_interface() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_init
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
H5FD_sid_init(void)
{
    hid_t ret_value;            /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if(H5I_VFL != H5I_get_type(H5FD_SID_g))
        H5FD_SID_g = H5FD_register(&H5FD_sid_g, sizeof(H5FD_class_t), FALSE);

    /* Set return value */
    ret_value = H5FD_SID_g;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_sid_init() */


/*---------------------------------------------------------------------------
 * Function:    H5FD_sid_term
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
H5FD_sid_term(void)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Reset VFL ID */
    H5FD_SID_g = 0;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5FD_sid_term() */


/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_sid
 *
 * Purpose:     Modify the file access property list to use the H5FD_SID
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
H5Pset_fapl_sid(hid_t fapl_id)
{
    H5P_genplist_t *plist;      /* Property list pointer */
    herr_t ret_value;

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", fapl_id);

    if(NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list")

    ret_value = H5P_set_driver(plist, H5FD_SID, NULL);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_sid() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_open
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
H5FD_sid_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5FD_sid_t     *file       = NULL;     /* sec2 VFD info            */
    int             fd          = -1;       /* File descriptor          */
    int             o_flags;                /* Flags for open() call    */
#ifdef H5_HAVE_WIN32_API
    struct _BY_HANDLE_FILE_INFORMATION fileinfo;
#endif
    h5_stat_t       sb;
    H5FD_t          *ret_value;             /* Return value             */

    int log_fd;
    char log_name[H5FD_MAX_FILENAME_LEN];

    FUNC_ENTER_NOAPI_NOINIT

    printf("H5FD_sid_open(%s)\n", name);

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

	if(H5F_ACC_RDWR & flags) {

		HDsnprintf(log_name, H5FD_MAX_FILENAME_LEN, "%s.do_not_delete.journal", name);

		if(HDaccess(log_name, F_OK)) {
			printf("Replay log\n");
			if(H5FD_sid_replay_log(name, log_name) < 0) {
				printf("Replay is dropped\n");
			}
		}

	} else {
		/* read only */
		log_fd = -1;
	}

    /* Open the file */
    if((fd = HDopen(name, o_flags, 0666)) < 0) {
        int myerrno = errno;
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to open file: name = '%s', errno = %d, error message = '%s', flags = %x, o_flags = %x", name, myerrno, HDstrerror(myerrno), flags, (unsigned)o_flags);
    } /* end if */

    if(HDfstat(fd, &sb) < 0)
        HSYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, NULL, "unable to fstat file")

    /* Create the new file struct */
    if(NULL == (file = H5FL_CALLOC(H5FD_sid_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate file struct")

    printf("Create file handler %p\n", file);

	if(H5F_ACC_RDWR & flags) {

		printf("Clear log\n");

		/* the journal exist and is readable and writable */
		if((log_fd = HDopen(log_name, O_RDWR | O_TRUNC | O_CREAT, 0666)) < 0) {
		   int myerrno = errno;
		   HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to open file: name = '%s', errno = %d, error message = '%s', flags = %x, o_flags = %x", log_name, myerrno, HDstrerror(myerrno), flags, (unsigned)o_flags);
		}

	} else {
		/* read only */
		log_fd = -1;
	}

    file->fd = fd;
	file->log_fd = log_fd;

    H5_ASSIGN_OVERFLOW(file->eof, sb.st_size, h5_stat_size_t, haddr_t);
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

	HDstrncpy(file->log_filename, log_name, sizeof(file->log_filename));
	file->log_filename[sizeof(file->log_filename) - 1] = '\0';

	/** init map **/
	file->map = H5FD_sid_vfm_new(sb.st_size);

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
            if(H5P_get(plist, H5F_ACS_FAMILY_TO_SEC2_NAME, &file->fam_to_sid) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get property of changing family to sec2")
    } /* end if */

    /* Set return value */
    ret_value = (H5FD_t*)file;

done:
    if(NULL == ret_value) {
        if(fd >= 0)
            HDclose(fd);
        if(file)
            file = H5FL_FREE(H5FD_sid_t, file);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_sid_open() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_close
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
H5FD_sid_close(H5FD_t *_file)
{
    H5FD_sid_t *file = (H5FD_sid_t *)_file;
    herr_t      ret_value = SUCCEED;                /* Return value */
    H5FD_sid_log_msg_t hdr;

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity check */
    HDassert(file);

    printf("H5FD_sid_close(%p, %s)\n", file, file->filename);

    hdr.type = H5FD_SID_MSG_CLOSE_FILE;
    hdr.m_close.size = 0;

    /** ensure log are closed and written on disk **/
    if(HDwrite(file->log_fd, &hdr, sizeof(H5FD_sid_log_msg_t)) < 0) {
    	HSYS_GOTO_ERROR(H5E_IO, H5E_CANTCLOSEFILE, FAIL, "unable to write log file")
    }
    fsync(file->log_fd);
    close(file->log_fd);

    /** replay log to write on file **/
	if(H5FD_sid_replay_log(file->filename, file->log_filename) < 0) {
		printf("FAIL to read log\n");
	} else {
	    HDunlink(file->log_filename);
	}

    /* Close the underlying file */
    if(HDclose(file->fd) < 0)
        HSYS_GOTO_ERROR(H5E_IO, H5E_CANTCLOSEFILE, FAIL, "unable to close file")

    /** remove temporary file **/


    /* Release the file info */
    file = H5FL_FREE(H5FD_sid_t, file);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_sid_close() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_cmp
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
H5FD_sid_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_sid_t   *f1 = (const H5FD_sid_t *)_f1;
    const H5FD_sid_t   *f2 = (const H5FD_sid_t *)_f2;
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
} /* end H5FD_sid_cmp() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_query
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
H5FD_sid_query(const H5FD_t *_file, unsigned long *flags /* out */)
{
    const H5FD_sid_t	*file = (const H5FD_sid_t *)_file;    /* sec2 VFD info */

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
        if(file && file->fam_to_sid)
            *flags |= H5FD_FEAT_IGNORE_DRVRINFO; /* Ignore the driver info when file is opened (which eliminates it) */
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD_sid_query() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_get_eoa
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
H5FD_sid_get_eoa(const H5FD_t *_file, H5FD_mem_t UNUSED type)
{
    const H5FD_sid_t	*file = (const H5FD_sid_t *)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(file->eoa)
} /* end H5FD_sid_get_eoa() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_set_eoa
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
H5FD_sid_set_eoa(H5FD_t *_file, H5FD_mem_t UNUSED type, haddr_t addr)
{
    H5FD_sid_t	*file = (H5FD_sid_t *)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    file->eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD_sid_set_eoa() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_get_eof
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
H5FD_sid_get_eof(const H5FD_t *_file)
{
    const H5FD_sid_t   *file = (const H5FD_sid_t *)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(MAX(file->eof, file->eoa))
} /* end H5FD_sid_get_eof() */


/*-------------------------------------------------------------------------
 * Function:       H5FD_sid_get_handle
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
H5FD_sid_get_handle(H5FD_t *_file, hid_t UNUSED fapl, void **file_handle)
{
    H5FD_sid_t         *file = (H5FD_sid_t *)_file;
    herr_t              ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    if(!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file handle not valid")

    *file_handle = &(file->fd);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_sid_get_handle() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_read
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
H5FD_sid_read(H5FD_t *_file, H5FD_mem_t UNUSED type, hid_t UNUSED dxpl_id,
    haddr_t addr, size_t size, void *buf /*out*/)
{
    H5FD_sid_t     *file       = (H5FD_sid_t *)_file;
    herr_t          ret_value   = SUCCEED;                  /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    printf("H5FD_sid_read(%ld, %ld)\n", addr, size);

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
            bytes_read = H5FD_sid_virtual_read(file, buf, addr, bytes_in);
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
} /* end H5FD_sid_read() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_write
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
H5FD_sid_write(H5FD_t *_file, H5FD_mem_t UNUSED type, hid_t UNUSED dxpl_id,
                haddr_t addr, size_t size, const void *buf)
{
    H5FD_sid_t     *file       = (H5FD_sid_t *)_file;
    herr_t          ret_value   = SUCCEED;                  /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    printf("H5FD_sid_write(%ld, %ld)\n", addr, size);

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
        	bytes_wrote = H5FD_sid_virtual_write(file, buf, addr, bytes_in);

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
} /* end H5FD_sid_write() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_sid_truncate
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
H5FD_sid_truncate(H5FD_t *_file, hid_t UNUSED dxpl_id, hbool_t UNUSED closing)
{
    H5FD_sid_t *file = (H5FD_sid_t *)_file;
    herr_t ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(file);

    printf("truncate\n");

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
        if(-1 == H5FD_sid_virtual_truncate(file, (size_t)file->eoa))
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
} /* end H5FD_sid_truncate() */
