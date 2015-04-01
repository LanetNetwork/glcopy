/* vim: set tabstop=4:softtabstop=4:shiftwidth=4:noexpandtab */

#include <errno.h>
#include <getopt.h>
#include <glusterfs/api/glfs.h>
#include <pfcq.h>
#include <pfgfq.h>
#include <pfpthq.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>

#define DEFAULT_WORKERS_COUNT	3

typedef struct gl_node
{
	glfs_t* fs;
	char* protocol;
	char* server;
	char* volume;
	char* path;
	int port;
	int padding; /* struct padding */
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

typedef struct copy_context
{
	fop_args_t fop;
	char buffer[IO_CHUNK_SIZE];
	ssize_t read_size;
	glfs_fd_t* src_fd;
	glfs_fd_t* dst_fd;
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

	// protocol:server:port:volume:path
	part = strsep(&p, ":");
	if (unlikely(!part))
		goto err;
	(*_list[*_counter]).protocol = pfcq_strdup(part);
	part = strsep(&p, ":");
	if (unlikely(!part))
		goto err;
	(*_list[*_counter]).server = pfcq_strdup(part);
	part = strsep(&p, ":");
	if (unlikely(!part))
		goto err;
	if (unlikely(!pfcq_isnumber(part)))
		goto err;
	(*_list[*_counter]).port = atoi(part);
	part = strsep(&p, ":");
	if (unlikely(!part))
		goto err;
	(*_list[*_counter]).volume = pfcq_strdup(part);
	part = strsep(&p, ":");
	if (unlikely(!part))
		goto err;
	(*_list[*_counter]).path = pfcq_strdup(part);

	(*_counter)++;
	goto lfree;

err:
	pfcq_free((*_list[*_counter]).protocol);
	pfcq_free((*_list[*_counter]).server);
	pfcq_free((*_list[*_counter]).volume);
	pfcq_free((*_list[*_counter]).path);
	(*_list[*_counter]).port = 0;

lfree:
	pfcq_free(tmp_p);

out:
	return;
}

static void open_node(gl_node_t* _list, unsigned int _index, char* _log)
{
	if (unlikely(_list[_index].protocol[0] == 0))
		_list[_index].protocol = pfcq_strdup(GLFS_DEFAULT_PROTOCOL);
	if (unlikely(!_list[_index].server))
		panic("No GlusterFS server specified!");
	if (unlikely(_list[_index].port == 0))
		_list[_index].port = GLFS_DEFAULT_PORT;
	if (unlikely(!_list[_index].volume))
		panic("No GlusterFS volume specified!");
	if (unlikely(!_list[_index].path))
		panic("No GlusterFS source path specified!");

	_list[_index].fs = glfs_new(_list[_index].volume);
	if (unlikely(!_list[_index].fs))
		panic("glfs_new");
	if (unlikely(glfs_set_volfile_server(_list[_index].fs, _list[_index].protocol, _list[_index].server, _list[_index].port)))
		panic("glfs_set_volfile_server");
	if (unlikely(glfs_set_logging(_list[_index].fs, _log, GLFS_DEFAULT_VERBOSITY)))
		warning("glfs_set_logging");

	if (unlikely(glfs_init(_list[_index].fs)))
		panic("glfs_init");

	return;
}

static void close_node(gl_node_t* _list, unsigned int _index)
{
	if (unlikely(glfs_fini(_list[_index].fs)))
		warning("glfs_fini");

	return;
}

static void delete_node(gl_node_t** _list, unsigned int _index)
{
	pfcq_free((*_list[_index]).protocol);
	pfcq_free((*_list[_index]).server);
	(*_list[_index]).port = 0;
	pfcq_free((*_list[_index]).volume);
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
	glfs_mkdir_safe(dst_nodes[_context->fop.dst_index].fs, _context->fop.dst_path, _context->fop.mode);

	pfcq_free(_context->fop.dst_path);

	return;
}

static void copyfile_node(copy_context_t* _context)
{
	_context->fop.dst_path = getdstpath_node(_context->fop.dst_index, _context->fop.src_path, _context->fop.src_index);

	verbose("Copying file %s from src-%u to %s on node dst-%u\n", _context->fop.src_path, _context->fop.src_index, _context->fop.dst_path, _context->fop.dst_index);
	_context->src_fd = glfs_open(src_nodes[_context->fop.src_index].fs, _context->fop.src_path, O_RDONLY);
	if (unlikely(!_context->src_fd))
		goto lfree;
	_context->dst_fd = glfs_creat(dst_nodes[_context->fop.dst_index].fs, _context->fop.dst_path, O_CREAT | O_WRONLY | O_TRUNC, _context->fop.mode);
	if (unlikely(!_context->src_fd))
		goto lfree;

	while ((_context->read_size = glfs_read(_context->src_fd, _context->buffer, IO_CHUNK_SIZE, 0)) > 0)
		glfs_write(_context->dst_fd, _context->buffer, _context->read_size, 0);

lfree:
	if (likely(_context->dst_fd))
		glfs_close(_context->dst_fd);
	if (likely(_context->src_fd))
		glfs_close(_context->src_fd);

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

static void walk_nodes_dentry_handler(glfs_t* _fs, const char* _path, struct dirent* _dentry, struct stat* _sb, void* _data, unsigned int _level)
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
		walk_dir_generic(_fs, _path, walk_nodes_dentry_handler, NULL, _data, 0);
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
		glfs_mkdir_safe(dst_nodes[i].fs, dst_nodes[i].path, CHMOD_755);
	}

	for (unsigned int i = 0; i < src_nodes_count; i++)
		walk_dir_generic(src_nodes[i].fs, src_nodes[i].path, walk_nodes_dentry_handler, NULL, &i, 0);

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

