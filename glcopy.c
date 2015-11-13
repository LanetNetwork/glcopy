/* vim: set tabstop=4:softtabstop=4:shiftwidth=4:noexpandtab */

/*
 * glcopy - utility to perform massive files copy over GlusterFS shares
 * Copyright (C) 2015 Lanet Network
 * Programmed by Oleksandr Natalenko <o.natalenko@lanet.ua>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3 of the License
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <errno.h>
#include <getopt.h>
#include <glusterfs/api/glfs.h>
#include <libcephfs.h>
#include <pfcfsq.h>
#include <pfcq.h>
#include <pfgfq.h>
#include <pfpthq.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>

#define DEFAULT_WORKERS_COUNT	3

typedef enum glc_node_type
{
	GLCNT_CFS,
	GLCNT_GLFS
} glc_node_type_t;

typedef struct glc_cfs_node
{
	struct ceph_mount_info* fs;
	char* mons;
	char* id;
	char* keyring_file;
	char* root;
} glc_cfs_node_t;

typedef struct glc_glfs_node
{
	glfs_t* fs;
	char* protocol;
	char* server;
	char* volume;
	int port;
	int __padding1; /* struct padding */
} glc_glfs_node_t;

typedef union glc_node
{
	glc_cfs_node_t cfs_node;
	glc_glfs_node_t glfs_node;
} glc_node_t;

typedef struct gl_node
{
	glc_node_type_t node_type;
	int __padding1; /* struct padding */
	glc_node_t node;
	char* path;
} gl_node_t;

typedef struct fop_args
{
	char* src_path;
	char* dst_path;
	unsigned int src_index;
	unsigned int dst_index;
	mode_t mode;
	int padding; /* struct padding */
} fop_args_t;

typedef struct mkdir_context
{
	fop_args_t fop;
} mkdir_context_t;

typedef union glc_fd_hd
{
	glfs_fd_t* glfs_fd_hd;
	int cfs_fd_hd;
} glc_fd_hd_t;

typedef struct copy_context
{
	fop_args_t fop;
	char buffer[IO_CHUNK_SIZE];
	ssize_t read_size;
	glc_fd_hd_t src_fd;
	glc_fd_hd_t dst_fd;
} copy_context_t;

static gl_node_t* src_nodes = NULL;
static gl_node_t* dst_nodes = NULL;
static unsigned int src_nodes_count;
static unsigned int dst_nodes_count;
static pfpthq_pool_t* pool = NULL;

static void add_node(gl_node_t** _list, unsigned int* _counter, const char* _uri)
{
	char* tmp_p = NULL;
	char* p = NULL;
	char* part = NULL;

	if (!*_list)
		*_list = pfcq_alloc(sizeof(gl_node_t));
	else
		*_list = pfcq_realloc(*_list, (*_counter + 1) * sizeof(gl_node_t));

	p = pfcq_strdup(_uri);
	tmp_p = p;
	if (unlikely(!tmp_p))
		goto out;

	// CephFS: cfs:mons:id:keyring_file:root:path
	// GlusterFS: glfs:protocol:server:port:volume:path
	part = strsep(&p, ":");
	if (unlikely(!part))
		goto lfree;
	if (strcmp(part, "cfs") == 0)
	{
		(*_list[*_counter]).node_type = GLCNT_CFS;
		part = strsep(&p, ":");
		if (unlikely(!part))
			goto cfs_err;
		(*_list[*_counter]).node.cfs_node.mons = pfcq_strdup(part);
		part = strsep(&p, ":");
		if (unlikely(!part))
			goto cfs_err;
		(*_list[*_counter]).node.cfs_node.id = pfcq_strdup(part);
		part = strsep(&p, ":");
		if (unlikely(!part))
			goto cfs_err;
		(*_list[*_counter]).node.cfs_node.keyring_file = pfcq_strdup(part);
		part = strsep(&p, ":");
		if (unlikely(!part))
			goto cfs_err;
		(*_list[*_counter]).node.cfs_node.root = pfcq_strdup(part);
		part = strsep(&p, ":");
		if (unlikely(!part))
			goto cfs_err;
		(*_list[*_counter]).path = pfcq_strdup(part);
		goto cfs_lfree;

cfs_err:
		pfcq_free((*_list[*_counter]).node.cfs_node.mons);
		pfcq_free((*_list[*_counter]).node.cfs_node.id);
		pfcq_free((*_list[*_counter]).node.cfs_node.keyring_file);
		pfcq_free((*_list[*_counter]).node.cfs_node.root);
		pfcq_free((*_list[*_counter]).path);
cfs_lfree:
		__noop;

	} else if (strcmp(part, "glfs") == 0)
	{
		(*_list[*_counter]).node_type = GLCNT_GLFS;
		part = strsep(&p, ":");
		if (unlikely(!part))
			goto glfs_err;
		(*_list[*_counter]).node.glfs_node.protocol = pfcq_strdup(part);
		part = strsep(&p, ":");
		if (unlikely(!part))
			goto glfs_err;
		(*_list[*_counter]).node.glfs_node.server = pfcq_strdup(part);
		part = strsep(&p, ":");
		if (unlikely(!part))
			goto glfs_err;
		if (unlikely(!pfcq_isnumber(part)))
			goto glfs_err;
		(*_list[*_counter]).node.glfs_node.port = atoi(part);
		part = strsep(&p, ":");
		if (unlikely(!part))
			goto glfs_err;
		(*_list[*_counter]).node.glfs_node.volume = pfcq_strdup(part);
		part = strsep(&p, ":");
		if (unlikely(!part))
			goto glfs_err;
		(*_list[*_counter]).path = pfcq_strdup(part);
		goto glfs_lfree;

glfs_err:
		pfcq_free((*_list[*_counter]).node.glfs_node.protocol);
		pfcq_free((*_list[*_counter]).node.glfs_node.server);
		pfcq_free((*_list[*_counter]).node.glfs_node.volume);
		pfcq_free((*_list[*_counter]).path);
		(*_list[*_counter]).node.glfs_node.port = 0;
glfs_lfree:
		__noop;
	} else
		panic("Unknown FS type specified");

	(*_counter)++;

lfree:
	pfcq_free(tmp_p);

out:
	return;
}

static void open_node(gl_node_t* _list, unsigned int _index, char* _log)
{
	switch (_list[_index].node_type)
	{
		case GLCNT_CFS:
			if (unlikely(!_list[_index].node.cfs_node.mons))
				panic("No CephFS monitors specified!");
			if (unlikely(!_list[_index].node.cfs_node.id))
				panic("No CephFS ID specified!");
			if (unlikely(!_list[_index].node.cfs_node.keyring_file))
				panic("No CephFS keyring file specified!");
			if (unlikely(!_list[_index].node.cfs_node.root))
				panic("No CephFS root specified!");
			if (unlikely(!_list[_index].path))
				panic("No CephFS path specified!");

			_list[_index].node.cfs_node.fs = cfs_mount(
				_list[_index].node.cfs_node.mons,
				_list[_index].node.cfs_node.id,
				_list[_index].node.cfs_node.keyring_file,
				_list[_index].node.cfs_node.root);

			if (unlikely(!_list[_index].node.cfs_node.fs))
				panic("cfs_mount");
			break;
		case GLCNT_GLFS:
			if (unlikely(_list[_index].node.glfs_node.protocol[0] == 0))
				_list[_index].node.glfs_node.protocol = pfcq_strdup(GLFS_DEFAULT_PROTOCOL);
			if (unlikely(!_list[_index].node.glfs_node.server))
				panic("No GlusterFS server specified!");
			if (unlikely(_list[_index].node.glfs_node.port == 0))
				_list[_index].node.glfs_node.port = GLFS_DEFAULT_PORT;
			if (unlikely(!_list[_index].node.glfs_node.volume))
				panic("No GlusterFS volume specified!");
			if (unlikely(!_list[_index].path))
				panic("No GlusterFS source path specified!");

			_list[_index].node.glfs_node.fs = glfs_new(_list[_index].node.glfs_node.volume);
			if (unlikely(!_list[_index].node.glfs_node.fs))
				panic("glfs_new");
			if (unlikely(glfs_set_volfile_server(
				_list[_index].node.glfs_node.fs, _list[_index].node.glfs_node.protocol, _list[_index].node.glfs_node.server, _list[_index].node.glfs_node.port)))
				panic("glfs_set_volfile_server");
			if (unlikely(glfs_set_logging(_list[_index].node.glfs_node.fs, _log, GLFS_DEFAULT_VERBOSITY)))
				warning("glfs_set_logging");

			if (unlikely(glfs_init(_list[_index].node.glfs_node.fs)))
				panic("glfs_init");
			break;
		default:
			panic("Not implemented");
			break;
	}

	return;
}

static void close_node(gl_node_t* _list, unsigned int _index)
{
	switch (_list[_index].node_type)
	{
		case GLCNT_CFS:
			if (unlikely(cfs_unmount(_list[_index].node.cfs_node.fs) == -1))
				warning("cfs_unmount");
			break;
		case GLCNT_GLFS:
			if (unlikely(glfs_fini(_list[_index].node.glfs_node.fs)))
				warning("glfs_fini");
			break;
		default:
			panic("Not implemented");
			break;
	}

	return;
}

static void delete_node(gl_node_t** _list, unsigned int _index)
{
	switch ((*_list[_index]).node_type)
	{
		case GLCNT_CFS:
			pfcq_free((*_list[_index]).node.cfs_node.mons);
			pfcq_free((*_list[_index]).node.cfs_node.id);
			pfcq_free((*_list[_index]).node.cfs_node.keyring_file);
			pfcq_free((*_list[_index]).node.cfs_node.root);
			break;
		case GLCNT_GLFS:
			pfcq_free((*_list[_index]).node.glfs_node.protocol);
			pfcq_free((*_list[_index]).node.glfs_node.server);
			(*_list[_index]).node.glfs_node.port = 0;
			pfcq_free((*_list[_index]).node.glfs_node.volume);
			break;
		default:
			panic("Not implemented");
			break;
	}
	pfcq_free((*_list[_index]).path);
	pfcq_free(_list[_index]);

	return;
}

static char* getdstpath_node(unsigned int _dst_index, const char* _src_path, unsigned int _src_index)
{
	size_t src_path_length = strlen(_src_path);
	size_t src_nodes_src_index_path_length = strlen(src_nodes[_src_index].path);
	size_t dst_nodes_dst_index_path_length = strlen(dst_nodes[_dst_index].path);
	size_t dst_path_length = src_path_length - src_nodes_src_index_path_length + dst_nodes_dst_index_path_length;
	char* dst_path = pfcq_alloc(dst_path_length + 1);
	strcpy(dst_path, dst_nodes[_dst_index].path);
	if (dst_nodes[_dst_index].path[strlen(dst_nodes[_dst_index].path) - 1] != '/')
	{
		dst_path = pfcq_realloc(dst_path, dst_path_length + 2);
		strcat(dst_path, "/");
	}
	if (src_nodes[_src_index].path[strlen(src_nodes[_src_index].path) - 1] == '/')
		strcat(dst_path, _src_path + strlen(src_nodes[_src_index].path));
	else
		strcat(dst_path, _src_path + strlen(src_nodes[_src_index].path) + 1);

	return dst_path;
}

static void mkdir_node(mkdir_context_t* _context)
{
	_context->fop.dst_path = getdstpath_node(_context->fop.dst_index, _context->fop.src_path, _context->fop.src_index);

	verbose("Making directory %s on node dst-%u\n", _context->fop.dst_path, _context->fop.dst_index);
	switch (dst_nodes[_context->fop.dst_index].node_type)
	{
		case GLCNT_CFS:
			cfs_mkdir_safe(dst_nodes[_context->fop.dst_index].node.cfs_node.fs, _context->fop.dst_path, _context->fop.mode);
			break;
		case GLCNT_GLFS:
			glfs_mkdir_safe(dst_nodes[_context->fop.dst_index].node.glfs_node.fs, _context->fop.dst_path, _context->fop.mode);
			break;
		default:
			panic("Not implemented");
			break;
	}

	pfcq_free(_context->fop.dst_path);

	return;
}

static void copyfile_node(copy_context_t* _context)
{
	_context->fop.dst_path = getdstpath_node(_context->fop.dst_index, _context->fop.src_path, _context->fop.src_index);

	verbose("Copying file %s from src-%u to %s on node dst-%u\n", _context->fop.src_path, _context->fop.src_index, _context->fop.dst_path, _context->fop.dst_index);

	switch (src_nodes[_context->fop.src_index].node_type)
	{
		case GLCNT_CFS:
			_context->src_fd.cfs_fd_hd = ceph_open(src_nodes[_context->fop.src_index].node.cfs_node.fs, _context->fop.src_path, O_RDONLY, 0);
			if (unlikely(!_context->src_fd.glfs_fd_hd))
				goto cfs_err;
			_context->dst_fd.cfs_fd_hd = ceph_open(dst_nodes[_context->fop.dst_index].node.cfs_node.fs, _context->fop.dst_path, O_CREAT | O_WRONLY | O_TRUNC, _context->fop.mode);
			if (unlikely(!_context->dst_fd.cfs_fd_hd))
				goto cfs_err;

			goto cfs_out;

cfs_err:
			if (likely(_context->src_fd.cfs_fd_hd))
				ceph_close(src_nodes[_context->fop.src_index].node.cfs_node.fs, _context->src_fd.cfs_fd_hd);
			if (likely(_context->dst_fd.cfs_fd_hd))
				ceph_close(dst_nodes[_context->fop.dst_index].node.cfs_node.fs, _context->dst_fd.cfs_fd_hd);
			goto out;

cfs_out:
			break;
		case GLCNT_GLFS:
			_context->src_fd.glfs_fd_hd = glfs_open(src_nodes[_context->fop.src_index].node.glfs_node.fs, _context->fop.src_path, O_RDONLY);
			if (unlikely(!_context->src_fd.glfs_fd_hd))
				goto glfs_err;
			_context->dst_fd.glfs_fd_hd = glfs_creat(dst_nodes[_context->fop.dst_index].node.glfs_node.fs, _context->fop.dst_path, O_CREAT | O_WRONLY | O_TRUNC, _context->fop.mode);
			if (unlikely(!_context->dst_fd.glfs_fd_hd))
				goto glfs_err;

			goto glfs_out;

glfs_err:
			if (likely(_context->src_fd.glfs_fd_hd))
				glfs_close(_context->src_fd.glfs_fd_hd);
			if (likely(_context->dst_fd.glfs_fd_hd))
				glfs_close(_context->dst_fd.glfs_fd_hd);
			goto out;

glfs_out:
			break;
		default:
			panic("Not implemented");
			break;
	}

	for (;;)
	{
		switch (src_nodes[_context->fop.src_index].node_type)
		{
			case GLCNT_CFS:
				_context->read_size = ceph_read(src_nodes[_context->fop.src_index].node.cfs_node.fs, _context->src_fd.cfs_fd_hd, _context->buffer, IO_CHUNK_SIZE, -1);
				break;
			case GLCNT_GLFS:
				_context->read_size = glfs_read(_context->src_fd.glfs_fd_hd, _context->buffer, IO_CHUNK_SIZE, 0);
				break;
			default:
				panic("Not implemented");
				break;
		}

		if (_context->read_size <= 0)
			break;

		switch (dst_nodes[_context->fop.dst_index].node_type)
		{
			case GLCNT_CFS:
				ceph_write(dst_nodes[_context->fop.dst_index].node.cfs_node.fs, _context->dst_fd.cfs_fd_hd, _context->buffer, _context->read_size, -1);
				break;
			case GLCNT_GLFS:
				glfs_write(_context->dst_fd.glfs_fd_hd, _context->buffer, _context->read_size, 0);
				break;
			default:
				panic("Not implemented");
				break;
		}
	}

	switch (src_nodes[_context->fop.src_index].node_type)
	{
		case GLCNT_CFS:
			if (likely(_context->src_fd.cfs_fd_hd > 0))
				ceph_close(src_nodes[_context->fop.src_index].node.cfs_node.fs, _context->src_fd.cfs_fd_hd);
			break;
		case GLCNT_GLFS:
			if (likely(_context->src_fd.glfs_fd_hd))
				glfs_close(_context->src_fd.glfs_fd_hd);
			break;
		default:
			panic("Not implemented");
			break;
	}

	switch (dst_nodes[_context->fop.dst_index].node_type)
	{
		case GLCNT_CFS:
			if (likely(_context->dst_fd.cfs_fd_hd > 0))
				ceph_close(dst_nodes[_context->fop.dst_index].node.cfs_node.fs, _context->dst_fd.cfs_fd_hd);
			break;
		case GLCNT_GLFS:
			if (likely(_context->dst_fd.glfs_fd_hd))
				glfs_close(_context->dst_fd.glfs_fd_hd);
			break;
		default:
			panic("Not implemented");
			break;
	}

out:
	pfcq_free(_context->fop.dst_path);

	return;
}

static void* mkdir_worker(void* _data)
{
	mkdir_context_t* context = _data;
	if (unlikely(!context))
		goto lfree;

	mkdir_node(context);

	pfcq_free(context->fop.src_path);
	pfcq_free(context);

lfree:
	pfpthq_dec(pool);

	return NULL;
}

static void* copy_worker(void* _data)
{
	copy_context_t* context = _data;
	if (unlikely(!context))
		goto lfree;

	copyfile_node(context);

	pfcq_free(context->fop.src_path);
	pfcq_free(context);

lfree:
	pfpthq_dec(pool);

	return NULL;
}

static void cfs_walk_nodes_dentry_handler(struct ceph_mount_info* _fs, const char* _path, struct dirent* _dentry, struct stat* _sb, void* _data, unsigned int _level)
{
	(void)_dentry;
	(void)_level;
	pthread_t id;

	if (unlikely(S_ISDIR(_sb->st_mode)))
	{
		for (unsigned int i = 0; i < dst_nodes_count; i++)
		{
			mkdir_context_t* new_mkdir_context = pfcq_alloc(sizeof(mkdir_context_t));
			new_mkdir_context->fop.src_index = *((unsigned int*)_data);
			new_mkdir_context->fop.dst_index = i;
			new_mkdir_context->fop.src_path = pfcq_strdup(_path);
			new_mkdir_context->fop.mode = _sb->st_mode;
			pfpthq_inc(pool, &id, "mkdir worker", mkdir_worker, (void*)new_mkdir_context);
		}
		cfs_walk_dir_generic(_fs, _path, cfs_walk_nodes_dentry_handler, NULL, _data, 0);
	}
	else
	{
		for (unsigned int i = 0; i < dst_nodes_count; i++)
		{
			copy_context_t* new_copy_context = pfcq_alloc(sizeof(copy_context_t));
			new_copy_context->fop.src_index = *((unsigned int*)_data);
			new_copy_context->fop.dst_index = i;
			new_copy_context->fop.src_path = pfcq_strdup(_path);
			new_copy_context->fop.mode = _sb->st_mode;
			pfpthq_inc(pool, &id, "copy worker", copy_worker, (void*)new_copy_context);
		}
	}

	return;
}

static void glfs_walk_nodes_dentry_handler(glfs_t* _fs, const char* _path, struct dirent* _dentry, struct stat* _sb, void* _data, unsigned int _level)
{
	(void)_dentry;
	(void)_level;
	pthread_t id;

	if (unlikely(S_ISDIR(_sb->st_mode)))
	{
		for (unsigned int i = 0; i < dst_nodes_count; i++)
		{
			mkdir_context_t* new_mkdir_context = pfcq_alloc(sizeof(mkdir_context_t));
			new_mkdir_context->fop.src_index = *((unsigned int*)_data);
			new_mkdir_context->fop.dst_index = i;
			new_mkdir_context->fop.src_path = pfcq_strdup(_path);
			new_mkdir_context->fop.mode = _sb->st_mode;
			pfpthq_inc(pool, &id, "mkdir worker", mkdir_worker, (void*)new_mkdir_context);
		}
		glfs_walk_dir_generic(_fs, _path, glfs_walk_nodes_dentry_handler, NULL, _data, 0);
	}
	else
	{
		for (unsigned int i = 0; i < dst_nodes_count; i++)
		{
			copy_context_t* new_copy_context = pfcq_alloc(sizeof(copy_context_t));
			new_copy_context->fop.src_index = *((unsigned int*)_data);
			new_copy_context->fop.dst_index = i;
			new_copy_context->fop.src_path = pfcq_strdup(_path);
			new_copy_context->fop.mode = _sb->st_mode;
			pfpthq_inc(pool, &id, "copy worker", copy_worker, (void*)new_copy_context);
		}
	}

	return;
}

int main(int argc, char** argv)
{
	int be_verbose = 0;
	int do_debug = 0;
	int use_syslog = 0;
	int workers_count = DEFAULT_WORKERS_COUNT;
	char* glfs_log = NULL;

	src_nodes_count = 0;
	dst_nodes_count = 0;

	int opts;
	struct option longopts[] = {
		{"from",	required_argument,	NULL, 'a'},
		{"to",		required_argument,	NULL, 'b'},
		{"workers",	required_argument,	NULL, 'e'},
		{"verbose",	required_argument,	NULL, 'f'},
		{"debug",	no_argument,		NULL, 'c'},
		{"syslog",	no_argument,		NULL, 'd'},
		{0, 0, 0, 0}
	};

	while ((opts = getopt_long(argc, argv, "abefcd", longopts, NULL)) != -1)
		switch (opts)
		{
			case 'a':
				add_node(&src_nodes, &src_nodes_count, optarg);
				break;
			case 'b':
				add_node(&dst_nodes, &dst_nodes_count, optarg);
				break;
			case 'e':
				if (unlikely(!pfcq_isnumber(optarg)))
					panic("Wrong workers count!");
				workers_count = atoi(optarg);
				break;
			case 'f':
				be_verbose = 1;
				break;
			case 'c':
				do_debug = 1;
				break;
			case 'd':
				use_syslog = 1;
				break;
			default:
				panic("Unknown option occurred");
				break;
		}

	pfcq_debug_init(be_verbose, do_debug, use_syslog);
	glfs_log = pfcq_strdup(use_syslog ? DEV_NULL : DEV_STDERR);

	pool = pfpthq_init("workers", workers_count);

	for (unsigned int i = 0; i < src_nodes_count; i++)
		open_node(src_nodes, i, glfs_log);
	for (unsigned int i = 0; i < dst_nodes_count; i++)
	{
		open_node(dst_nodes, i, glfs_log);
		switch (dst_nodes[i].node_type)
		{
			case GLCNT_CFS:
				cfs_mkdir_safe(dst_nodes[i].node.cfs_node.fs, dst_nodes[i].path, CHMOD_755);
				break;
			case GLCNT_GLFS:
				glfs_mkdir_safe(dst_nodes[i].node.glfs_node.fs, dst_nodes[i].path, CHMOD_755);
				break;
			default:
				panic("Not implemented");
				break;
		}
	}

	for (unsigned int i = 0; i < src_nodes_count; i++)
	{
		switch (src_nodes[i].node_type)
		{
			case GLCNT_CFS:
				cfs_walk_dir_generic(src_nodes[i].node.cfs_node.fs, src_nodes[i].path, cfs_walk_nodes_dentry_handler, NULL, &i, 0);
				break;
			case GLCNT_GLFS:
				glfs_walk_dir_generic(src_nodes[i].node.glfs_node.fs, src_nodes[i].path, glfs_walk_nodes_dentry_handler, NULL, &i, 0);
				break;
			default:
				break;
		}
	}

	for (unsigned int i = 0; i < src_nodes_count; i++)
	{
		close_node(src_nodes, i);
		delete_node(&src_nodes, i);
	}
	for (unsigned int i = 0; i < dst_nodes_count; i++)
	{
		close_node(dst_nodes, i);
		delete_node(&dst_nodes, i);
	}

	pfpthq_wait(pool);
	pfpthq_done(pool);

	pfcq_debug_done();

	pfcq_free(glfs_log);

	exit(EX_OK);
}

