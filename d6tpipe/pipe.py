import os, shutil, datetime, time, fnmatch, re, copy, itertools
from pathlib import Path, PurePosixPath
import warnings, logging
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

from datetime import datetime
from tinydb import TinyDB, Query
from tinydb_serialization import SerializationMiddleware
import cachetools, expiringdict
from tqdm import tqdm

from d6tpipe.api import ConfigManager
from d6tpipe.tinydb_serializers import DateTimeSerializer
from d6tpipe.utils import filemd5, copytree
from d6tpipe.exceptions import *
import d6tcollect

#**********************************
# helpers
#**********************************

_cfg_mode_valid = ['default', 'new', 'mod', 'all']
_tdbserialization = SerializationMiddleware()
_tdbserialization.register_serializer(DateTimeSerializer(), 'TinyDate')


def _tinydb_last(db, getkey=None, sortkey='updated_at'):
    if db is None or len(db) == 0: return []
    r = sorted(db.all(), key=lambda k: k[sortkey])[-1]
    return r if getkey is None else r.get(getkey)


def _tinydb_insert(db, filessync, filesremote, fileslocal):
    if db is not None:
        db.insert({'updated_at': datetime.now(), 'action': 'push', 'sync':filessync, 'remote':filesremote, 'local': fileslocal})


def _files_new(filesfrom, filesto):
    filesfrom = [f['filename'] for f in filesfrom]
    filesto = [f['filename'] for f in filesto]
    return list(set(filesfrom)-set(filesto))

def _filenames(list_):
    return [d['filename'] for d in list_]

def _files_mod(filesfrom, filesto, key='crc'):
    def _tinydb_to_filedate_dict(query):
        return {k: v for (k, v) in [(d['filename'], d[key]) for d in query]}

    filesto = _tinydb_to_filedate_dict(filesto)
    filesfrom = _tinydb_to_filedate_dict(filesfrom)
    if filesto and filesfrom:
        return [k for k, v in filesfrom.items() if v != filesto.get(k,v)]
    else:
        return []

def _files_sort(files, sortby='filename'):
    return sorted(files, key=lambda k: k[sortby])

def _files_diff(filesfrom, filesto, mode, include, exclude, nrecent=0):
    if nrecent !=0:
        filesfrom_bydate = _files_sort(filesfrom, 'modified_at')
        filesfrom = filesfrom_bydate[-nrecent:] if nrecent>0 else filesfrom_bydate[:nrecent]
    if filesto:
        if mode == 'new':
            filesdiff = _files_new(filesfrom, filesto)
        elif mode == 'modified':
            filesdiff = _files_mod(filesfrom, filesto)
        elif mode == 'default':
            filesdiff = _files_new(filesfrom, filesto) + _files_mod(filesfrom, filesto)
        elif mode == 'all':
            filesdiff = _filenames(filesfrom)
        else:
            raise ValueError('Invalid pipe mode')
    else:
        filesdiff = _filenames(filesfrom)

    filesdiff = _apply_fname_filter(filesdiff, include, exclude)

    return filesdiff

def _apply_fname_filter(fnames, include, exclude):
    # todo: multi filter with *.csv|*.xls*|*.txt, split on |
    def helper(list_,filter_):
        return list(itertools.chain.from_iterable(fnmatch.filter(list_, ifilter) for ifilter in filter_.split('|')))
    if include:
        fnames = helper(fnames, include)

    if exclude:
        fnamesex = helper(fnames, exclude)
        fnames = list(set(fnames) - set(fnamesex))

    return fnames


#************************************************
# Pipe
#************************************************

class PipeBase(object, metaclass=d6tcollect.Collect):
    """

    Abstract class, don't use this directly

    """

    def __init__(self, name):
        if not re.match(r'^[a-zA-Z0-9-]+$', name):
            raise ValueError('Invalid pipe name, needs to be alphanumeric [a-zA-Z0-9-]')
        self.name = name

    def _getfilter(self, include=None, exclude=None):
        return include, exclude

    def scan_local_filenames(self):
        return self.scan_local(names_only=True)[1]

    def scan_local(self, files=None, fromdb=False, include=None, exclude=None, names_only=False, on_not_exist='warn'):
        """

        Get file attributes from local. To run before doing a pull/push

        Args:
            files (list): override list of filenames
            fromdb (bool): use files from local db, if false scans all files in pipe folder
            include (str): pattern of files to include, eg `*.csv` or `a-*.csv|b-*.csv`
            exclude (str): pattern of files to exclude
            names_only (bool): return filenames only, without attributes
            on_not_exist (bool): how to handle missing files when creating file attributes

        Returns:
            list: filenames with attributes

        """


        if files is None:
            if fromdb:
                files = _tinydb_last(self.dbfiles,'local')
                files = _filenames(files)
            else:
                files = [str(PurePosixPath(p.relative_to(self.dir))) for p in self.dirpath.glob('**/*') if not p.is_dir()]

            include, exclude = self._getfilter(include, exclude)
            files = _apply_fname_filter(files, include, exclude)
            files = sorted(files)
        if names_only: return [{'filename':s} for s in files], files

        def getattrib(fname):
            p = Path(self.dirpath)/fname
            if not p.exists():
                if on_not_exist=='warn':
                    warnings.warn('Local file {} does not exist'.format(fname))
                    return None
            dtmod = datetime.fromtimestamp(p.stat().st_mtime)
            crc = filemd5(p)
            return {'filename':fname, 'modified_at': dtmod, 'size': p.stat().st_size, 'crc': crc}

        filesall = [getattrib(fname) for fname in files]
        filesall = [o for o in filesall if o is not None]

        return filesall, files

    def filenames(self):
        """

        Files in local db

        Returns:
            list: filenames

        """
        files = _tinydb_last(self.dbfiles, 'local')
        files = _files_sort(files, self.sortby)
        return _filenames(files)

    def files(self, include=None, exclude=None, check_exists=True, aspathlib=True, fromdb=False):
        """

        Files in local db

        Args:
            include (str): pattern of files to include, eg `*.csv` or `a-*.csv|b-*.csv`
            exclude (str): pattern of files to exclude
            check_exists (bool): check files exist locally
            aspathlib (bool): return as pathlib object

        Returns:
            path: path to file, either `Pathlib` or `str`

        """

        fnames = _tinydb_last(self.dbfiles,'local') if fromdb else self.scan_local()[0]
        fnames = _files_sort(fnames, self.sortby)
        fnames = _apply_fname_filter(_filenames(fnames), include, exclude)
        fnames = [self.dirpath/f for f in fnames]

        if check_exists:
            [FileNotFoundError(fname) for fname in fnames if not fname.exists()]
        if not aspathlib:
            return [str(self.dirpath/f) for f in fnames]
        else:
            return fnames

    def files_one(self, name, check_exists=True):
        """

        File in local db

        Args:
            name (str): filename

        Returns:
            path: path to file, either `Pathlib` or `str`

        Notes: for now simply use `pipe.dirpath/'filename'`, this function is for future functionality

        """

        if not name in self.dbfiles.all() and check_exists:
            raise FileNotFoundError(name)
        fname = self.dirpath/name
        if not fname.exists() and check_exists:
            raise FileNotFoundError(name)

        return fname

    def import_files(self, files, subdir=None, move=False):
        """

        Import files to repo

        Args:
            files (list): list of files, eg from `glob.iglob('folder/**/*.csv')`
            subdir (str): sub directory to import into
            move (bool): move or copy

        """
        dstdir = self.dirpath/subdir if subdir else self.dirpath
        dstdir.mkdir(parents=True, exist_ok=True)
        if move:
            [shutil.move(ifile,dstdir/Path(ifile).name) for ifile in files]
        else:
            [shutil.copy(ifile,dstdir/Path(ifile).name) for ifile in files]

    def import_file(self, file, subdir=None, move=False):
        """

        Import a single file to repo

        Args:
            files (str): path to file
            subdir (str): sub directory to import into
            move (bool): move or copy

        """
        self.import_files([file], subdir, move)

    def import_dir(self, dir, move=False):
        """

        Import a directory including subdirs

        Args:
            dir (str): directory
            move (bool): move or copy

        """
        copytree(dir,self.dir, move)

    def delete_all_local(self, confirm=True, delete_all=None, ignore_errors=False):
        """

        Delete all local files and reset file database

        Args:
            confirm (bool): ask user to confirm delete
            delete_all (bool): delete all files or just synced files?
            ignore_errors (bool): ignore missing file errors

        """
        return self._empty_local(confirm, delete_all, ignore_errors)

    def _empty_local(self, confirm=True, delete_all=None, ignore_errors=False):
        if confirm:
            c = input('Confirm deleting files in '+self.dir+' (y/n)')
            if c=='n': return None
        else:
            c = 'y'
        if delete_all is None:
            d = input('Delete all files or just downloaded files (a/d)')
            delete_all = True if d=='a' else False
        if c=='y':
            if delete_all:
                shutil.rmtree(self.dir, ignore_errors=ignore_errors)
            else:
                [os.remove(self.dirpath/o['filename']) for o in _tinydb_last(self.dbfiles,'local')]
            self.dbfiles.purge()

class PipeLocal(PipeBase, metaclass=d6tcollect.Collect):
    """

    Managed data pipe, LOCAL mode for accessing local files ONLY

    Args:
        api (obj): API manager object
        name (str): name of the data pipe
        profile (str): name of profile to use
        filecfg (str): path to where config file is stored
        sortby (str): sort files this key. `filename`, `modified_at`, `size`

    """

    def __init__(self, name, config=None, profile=None, filecfg='~/d6tpipe/cfg.json', sortby='filename'):
        super().__init__(name)
        self.profile = 'default' if profile is None else profile
        if config is None:
            self.configmgr = ConfigManager(filecfg=filecfg, profile=self.profile)
            self.config = self.configmgr.load()
        else:
            self.config = config
            warnings.warn("Using manual config override, some api functions might not work")

        self.cfg_profile = self.config
        self.filerepo = self.cfg_profile['filerepo']
        self.dirpath = Path(self.filerepo)/name
        self.dir = str(self.dirpath) + os.sep
        self._db = TinyDB(self.cfg_profile['filedb'], storage=_tdbserialization)
        self.dbfiles = self._db.table(name+'-files')
        self.sortby = sortby

        # create db connection
        self._db = TinyDB(self.cfg_profile['filedb'], storage=_tdbserialization)
        self.dbfiles = self._db.table(name+'-files')
        self.dbconfig = self._db.table(name+'-cfg')

        self.settings = self.dbconfig.all()[-1]['pipe'] if self.dbconfig.all() else {}
        self.schema = self.settings.get('schema',{})

        print('Operating in local mode, use this to access local files, to run remote operations use `Pipe()`')

class Pipe(PipeBase, metaclass=d6tcollect.Collect):
    """
    Managed data pipe

    Args:
        api (obj): API manager object
        name (str): name of the data pipe. Has to be created first
        mode (str): sync mode
        sortby (str): sort files this key. `filename`, `modified_at`, `size`
        credentials (dict): override credentials

    Note:
        * mode: controls which files are synced
            * 'default': modified and new files
            * 'new': new files only
            * 'mod': modified files only
            * 'all': all files

    """

    def __init__(self, api, name, mode='default', sortby='filename', credentials=None):

        # set params
        super().__init__(name)
        self.api = api
        if not mode in _cfg_mode_valid:
            raise ValueError('Invalid mode, needs to be {}'.format(_cfg_mode_valid))
        self.mode = mode
        self.sortby = sortby

        # get remote details
        self.cnxnapi = api.cnxn
        self.cnxnpipe = self.cnxnapi.pipes._(name)
        self.settings = self.cnxnpipe.get()[1]
        if not self.settings:
            raise ValueError('pipe not found, make sure it was created')
        if self.settings['protocol'] not in ['ftp', 'sftp']:
            raise NotImplementedError('Unsupported protocol, only s3 and (s)ftp supported')
        self.remote_prefix = self.settings['location']
        self.settings['options'] = self.settings.get('options',{})
        self.encrypted_pipe = self.settings['options'].get('encrypted',False)
        if self.encrypted_pipe:
            self.settings = self.api.decode(self.settings)
        self.cfg_profile = api.cfg_profile
        self._set_dir(self.name)
        self.credentials_override = credentials

        # DDL
        self.schema = self.settings.get('schema',{})

        # create db connection
        self._db = TinyDB(self.cfg_profile['filedb'], storage=_tdbserialization)
        self.dbfiles = self._db.table(name+'-files')
        self.dbconfig = self._db.table(name+'-cfg')

        self._cache_scan = cachetools.TTLCache(maxsize=1, ttl=5*60)
        self._cache_creds = expiringdict.ExpiringDict(max_len=2, max_age_seconds=30*60)

        # connect msg
        msg = 'Successfully connected to pipe {} on remote {}. '.format(self.name,self.settings['remote'])
        if not 'write' in self.credentials:
            msg += ' Read only access'
        print(msg)
        self.dbconfig.upsert({'name': self.name, 'pipe': self.settings}, Query().name == self.name)

    def _set_dir(self, dir):
        self.dirpath = Path(self.api.filerepo)/dir
        self.dirpath.mkdir(parents=True, exist_ok=True)  # create dir if doesn't exist
        self.dir = str(self.dirpath) + os.sep

    def _getfilter(self, include, exclude):
        if include is None:
            include = self.settings['settings'].get('include')
        if exclude is None:
            exclude = self.settings['settings'].get('exclude')
        return include, exclude

    def setmode(self, mode):
        """

        Set sync mode

        Args:
            mode (str): sync mode

        Note:
            * mode: controls which files are synced
                * 'default': modified and new files
                * 'new': new files only
                * 'mod': modified files only
                * 'all': all files

        """
        assert mode in _cfg_mode_valid
        self.mode = mode

    def update_settings(self, config):
        """

        Update settings. Only keys present in the new dict will be updated, other parts of the config will be kept as is. In other words you can pass in a partial dict to update just the parts you need to be updated.

        Args:
            config (dict): updated config

        """

        self.settings.update(config)
        response, data = self.cnxnpipe.patch(config)
        return response, data

    def scan_remote(self, cached=True):
        """

        Get file attributes from remote. To run before doing a pull/push

        Args:
            cached (bool): use cached results

        Returns:
            list: filenames with attributes in remote

        """

        c = self._cache_scan.get(0)
        if cached and c is not None: return c

        filesall, filenames = self._list_luigi()
        response, data = (),filesall
        self._cache_scan[0] = (response, data)
        return response, data

        # only local scan for now
        raise NotImplementedError()
        self.cnxnpipe.files.post(request_body=filesall)
        if self.api.mode == 'local' or self.encrypted_remote or self.encrypted_pipe:
            pass
        else:
            response, data = self.cnxnpipe.scan.post()
            # some type of loop to wait until job is finished
            if response.status_code == 200:
                if 'job_id' in data: # async mode?
                    job_id = data['job_id']
                    while True:
                        response, data = self.cnxnpipe.jobs._(job_id).get()
                        if data['status'] == 'running': # todo: implement server status
                            time.sleep(5)
                        elif data['status'] == 'complete':
                            break
                        elif data['status'] == 'error':
                            raise NotImplementedError()
                        else:
                            raise ValueError(['Invalid task status', data['status']])

        response, data = self.cnxnpipe.files.get()
        self._cache_scan[0] = (response, data)
        return response, data

    def scan_remote_filenames(self):
        """

        Get file attributes from remote. To run before doing a pull/push

        Args:
            cached (bool): use cached results

        Returns:
            list: filenames with attributes

        """

        return _filenames(self.scan_remote()[1])

    def list_remote(self, filesnames=False, fromdb=False):
        """

        Get remote file attributes from local db

        Args:
            filesnames (bool): return only filenames not all file attributes
            fromdb (bool): if False do remote scan, if True get from local file db

        Returns:
            list: filenames with attributes

        """

        filesall = _tinydb_last(self.dbfiles,'local') if fromdb else self.scan_remote()[1]
        # filesall = self.cnxnpipe.files.get()[1] if fromdb else self.scan_remote()[1]
        filesall = _files_sort(filesall, self.sortby)
        filesall = _filenames(filesall) if filesnames else filesall
        return filesall

    def is_synced(self, israise=False):
        """

        Check if local is in sync with remote

        Args:
            israise (bool): raise an exception

        Returns:
            bool: pipe is updated

        """

        filespull = self.pull(dryrun=True)
        if filespull:
            if israise:
                raise PushError(['Remote has changes not pulled to local repo, run pull first', filespull])
            else:
                return False
        return True

    def pull_preview(self, files=None, include=None, exclude=None, nrecent=0, cached=True):
        """

        Preview of files to be pulled

        Args:
            files (list): override list of filenames
            include (str): pattern of files to include, eg `*.csv` or `a-*.csv|b-*.csv`
            exclude (str): pattern of files to exclude
            nrecent (int): use n newest files by mod date. 0 uses all files. Negative number uses n old files
            cached (bool): if True, use cached remote information, default 5mins. If False forces remote scan

        Returns:
            list: filenames with attributes

        """

        return self.pull(files=files, dryrun=True, include=include, exclude=exclude, nrecent=nrecent, cached=cached)

    def pull(self, files=None, dryrun=False, include=None, exclude=None, nrecent=0, merge_mode=None, cached=True):
        """

        Pull remote files to local

        Args:
            files (list): override list of filenames
            dryrun (bool): preview only
            include (str): pattern of files to include, eg `*.csv` or `a-*.csv|b-*.csv`
            exclude (str): pattern of files to exclude
            nrecent (int): use n newest files by mod date. 0 uses all files. Negative number uses n old files
            merge_mode (str): how to deal with pull conflicts ie files that changed both locally and remotely? 'keep' local files or 'overwrite' local files
            cached (bool): if True, use cached remote information, default 5mins. If False forces remote scan

        Returns:
            list: filenames with attributes

        """


        if self.settings_read_creds is None:
            raise ValueError('No read credentials provided. Either pass to pipe or update remote')

        if not cached:
            self._cache_scan.clear()

        filesremote = self.scan_remote()[1]

        if files is not None:
            filespull = files
        else:
            logging.debug(['remote files', filesremote])
            fileslocal = _files_sort(_tinydb_last(self.dbfiles,'remote'), self.sortby)
            filespull = _files_diff(filesremote, fileslocal, self.mode, include, exclude, nrecent)

        filespull_size = sum(f['size'] for f in filesremote if f['filename'] in filespull)
        print('pulling: {:.2f}MB'.format(filespull_size / 2 ** 20))
        if dryrun:
            return filespull

        # check if any local files have changed
        fileslocal = _tinydb_last(self.dbfiles,'local')
        fileslocalmod, _ = self.scan_local(files=[s for s in filespull if s in fileslocal])
        filesmod = _files_mod(fileslocal, fileslocalmod)

        if filesmod:
            if merge_mode is None:
                raise PullError('Modified files will be overwritten by pull, specify merge mode '+str(filesmod))
            elif merge_mode == 'keep':
                filespull = list(set(filespull)-set(filesmod))
            elif merge_mode == 'overwrite':
                pass
            else:
                raise ValueError('invalid merge mode')

        filessync = self._pullpush_luigi(filespull, 'get')

        # scan local files after pull
        fileslocal, _ = self.scan_local(files=_filenames(filessync))

        # update db
        _tinydb_insert(self.dbfiles, filessync, filesremote, fileslocal)
        self.dbconfig.upsert({'name': self.name, 'remote': self.settings, 'pipe': self.settings}, Query().name == self.name)

        # print README.md
        if 'README.md' in filessync:
            print('############### README ###############')
            with open(self.dirpath/'README.md', 'r') as fhandle:
                print(fhandle.read())
            print('############### README ###############')
        if 'LICENSE' in filessync:
            print('############### LICENSE ###############')
            with open(self.dirpath/'LICENSE', 'r') as fhandle:
                print(fhandle.read())
            print('############### LICENSE ###############')

        return filessync

    def push_preview(self, files=None, include=None, exclude=None, nrecent=0, cached=True):
        """

        Preview of files to be pushed

        Args:
            files (list): override list of filenames
            include (str): pattern of files to include, eg `*.csv` or `a-*.csv|b-*.csv`
            exclude (str): pattern of files to exclude
            nrecent (int): use n newest files by mod date. 0 uses all files. Negative number uses n old files
            cached (bool): if True, use cached remote information, default 5mins. If False forces remote scan

        Returns:
            list: filenames with attributes

        """

        return self.push(files=files, dryrun=True, include=include, exclude=exclude, nrecent=nrecent, cached=cached)

    def push(self, files=None, dryrun=False, fromdb=False, include=None, exclude=None, nrecent=0, cached=True):
        """

        Push local files to remote

        Args:
            files (list): override list of filenames
            dryrun (bool): preview only
            fromdb (bool): use files from local db, if false scans all files in pipe folder
            include (str): pattern of files to include, eg `*.csv` or `a-*.csv|b-*.csv`
            exclude (str): pattern of files to exclude
            nrecent (int): use n newest files by mod date. 0 uses all files. Negative number uses n old files
            cached (bool): if True, use cached remote information, default 5mins. If False forces remote scan

        Returns:
            list: filenames with attributes

        """

        if not self.settings_write_creds:
            raise ValueError('No write credentials provided. Check that you have provided write credentials or that you have write access')

        if not cached:
            self._cache_scan.clear()

        if files is not None:
            filespush = files
            fileslocal, _ = self.scan_local(fromdb=True)
        else:
            filesremote = _tinydb_last(self.dbfiles, 'local')
            fileslocal, _ = self.scan_local(fromdb=fromdb)
            if self.mode !='all': self.is_synced(israise=True)
            filespush = _files_diff(fileslocal, filesremote, self.mode, include, exclude, nrecent)

        filespush_size = sum(f['size'] for f in fileslocal if f['filename'] in filespush)
        if dryrun:
            print('pushing: {:.2f}MB'.format(filespush_size/2**20))
            return filespush

        filessync = self._pullpush_luigi(filespush, 'put')

        # get files on remote after push
        filesremote = self.scan_remote(cached=False)[1]

        _tinydb_insert(self.dbfiles, filessync, filesremote, fileslocal)

        return filessync

    def reset(self):
        """
        Resets by deleting all files and pulling
        """

        self._empty_local()
        self.pull()

    def remove_orphans(self, direction='local', dryrun=None):
        """

        Remove file orphans locally and/or remotely. When you remove files, they don't get synced because pull/push only looks at new or modified files. Use this to clean up any removed files.

        Args:
            direction (str): where to remove files
            dryrun (bool): preview only

        Note:
            * direction: 
                * 'local': remove files locally, ie files that exist on local but not in remote
                * 'remote': remove files remotely, ie files that exist on remote but not in local
                * 'both': combine local and remote

        """

        assert direction in ['both','local','remote']
        if dryrun is None:
            warnings.warn('dryrun active by default, to execute explicitly pass dryrun=False')
            dryrun = True

        fileslocal = self.scan_local(names_only=True, fromdb=False)[0]
        filesremote = self.scan_remote()[1]
        filesrmlocal = []
        filesrmremote = []

        if direction in ['local','both']:
            filesrmlocal = _files_new(fileslocal, filesremote)

        if direction in ['remote','both']:
            filesrmremote = _files_new(filesremote, fileslocal)

        if dryrun:
            return {'local': filesrmlocal, 'remote': filesrmremote}

        for fname in filesrmlocal:
            (self.dirpath/fname).unlink()

        if self.settings_write_creds is None:
            raise ValueError('No write credentials provided. Either pass to pipe or update remote')

        filesrmremote = self._pullpush_luigi(filesrmremote, 'remove')

        return {'local': filesrmlocal, 'remote': filesrmremote}

    def _list_luigi(self):
        remote_prefix = self._get_remote_prefix()

        def scan_s3():
            cnxn = self._connect()
            idxStart = len(remote_prefix)
            filesall = list(cnxn.listdir(remote_prefix,return_key=True))
            def s3path(o):
                return 's3://'+o.bucket_name+'/'+o.key
            filesall = [{'filename':s3path(o)[idxStart:], 'modified_at': str(o.last_modified), 'size':o.size, 'crc': o.e_tag.strip('\"')} for o in filesall if o.key[-1]!='/']
            return filesall

        def scan_ftp():
            try:
                from ftpsync.ftp_target import FtpTarget
            except:
                raise ModuleNotFoundError('pyftpsync not found. Run `pip install pyftpsync`')

            ftp_dir = remote_prefix
            idxStart = len(ftp_dir)

            cnxn = self._connect()
            try:
                cnxn.exists(ftp_dir)
            except Exception as e:
                if 'No such file or directory' in str(e):
                    cnxn._ftp_mkdirs(ftp_dir)
                    return []

            remote = FtpTarget(ftp_dir, self.settings['location'], username=self.settings_read_creds['username'], password=self.settings_read_creds['password'])
            remote.open()

            try:
                filesftp = list(remote.walk())
                filesall = [{'filename':(o.rel_path+'/'+o.name)[idxStart:], 'modified_at': datetime.fromtimestamp(o.mtime), 'size':o.size, 'crc': "{}-{}".format(str(o.mtime),str(o.size)) } for o in filesftp if not o.is_dir()]
            finally:
                remote.close()

            return filesall

        def scan_sftp():
            cnxn = self._connect()
            if cnxn.exists(remote_prefix):
                filesall = cnxn.listdir_attr(remote_prefix)
                filesall = [{'filename':o.relpath, 'modified_at': datetime.fromtimestamp(o.st_mtime), 'size':o.st_size, 'crc': "{}-{}".format(str(o.st_mtime),str(o.st_size)) } for o in filesall]
            else:
                filesall = []
            cnxn.close_del()
            return filesall

        if self.settings['protocol'] == 's3':
            filesall = scan_s3()
        elif self.settings['protocol'] == 'ftp':
            filesall = scan_ftp()
        elif self.settings['protocol'] == 'sftp':
            filesall = scan_sftp()
        else:
            raise NotImplementedError('only s3, ftp, sftp supported')

        include, exclude = self._getfilter(None,None)
        filenames = _filenames(filesall)
        filenames = _apply_fname_filter(filenames, include, exclude)
        filesall = [d for d in filesall if d['filename'] in filenames]

        filesall = sorted(filesall, key = lambda x: x['filename'])
        filenames = sorted(filenames)

        return filesall, filenames

    def _pullpush_luigi(self, files, op, cnxn=None):
        if cnxn is None:
            cnxn = self._connect(op in ['put','remove'])

        remote_prefix = self._get_remote_prefix()

        filessync = []
        pbar = ""
        for fname in tqdm(files):
            pbar = pbar + fname
            fnameremote = remote_prefix+fname
            fnamelocalpath = self.dirpath/fname
            fnamelocal = str(PurePosixPath(fnamelocalpath))
            if op=='put':
                cnxn.put(fnamelocal, fnameremote)
            elif op=='get':
                fnamelocalpath.parent.mkdir(parents=True, exist_ok=True)
                cnxn.get(fnameremote, fnamelocal)
            elif op=='remove':
                cnxn.remove(fnameremote)
            elif op=='exists':
                fname = cnxn.exists(fnameremote)
            else:
                raise ValueError('invalid luigi operation')

            logging.info('synced files {}'.format(fname))
            filessync.append(fname)

        self._disconnect(cnxn)

        return filessync

    def _connect(self, write=False):

        def _set_credentials(self, role):
            if credentials is None:
                credentials = self.settings.get('credentials', {})
            if any(credentials) or credentials is not None:
                if 'read' not in credentials:
                    credentials['read'] = credentials
                if 'write' not in credentials:
                    credentials['write'] = credentials
                self.credentials = credentials
            else:
                warnings.warn('No credentials were found!')

        if self.settings['protocol'] == 's3':
            from luigi.contrib.s3 import S3Client
            from d6tpipe.luigi.s3 import S3Client as S3ClientToken
            if write:
                if 'aws_session_token' in self.settings_write_creds:
                    cnxn = S3ClientToken(**self.settings_write_creds)
                else:
                    cnxn = S3Client(**self.settings_write_creds)
            else:
                if 'aws_session_token' in self.settings_read_creds:
                    cnxn = S3ClientToken(**self.settings_read_creds)
                else:
                    cnxn = S3Client(**self.settings_read_creds)
        elif self.settings['protocol'] == 'ftp':
            from d6tpipe.luigi.ftp import RemoteFileSystem
            if write:
                cnxn = RemoteFileSystem(self.settings['location'], self.settings_write_creds['username'], self.settings_write_creds['password'])
            else:
                cnxn = RemoteFileSystem(self.settings['location'], self.settings_read_creds['username'], self.settings_read_creds['password'])
        elif self.settings['protocol'] == 'sftp':
            from d6tpipe.luigi.ftp import RemoteFileSystem
            try:
                import pysftp
            except ImportError:
                raise ModuleNotFoundError('Please install pysftp to use SFTP.')
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None

            if write:
                cnxn = RemoteFileSystem(self.settings['location'], self.settings_write_creds['username'], self.settings_write_creds['password'], sftp=True, pysftp_conn_kwargs={'cnopts':cnopts})
            else:
                cnxn = RemoteFileSystem(self.settings['location'], self.settings_read_creds['username'], self.settings_read_creds['password'], sftp=True, pysftp_conn_kwargs={'cnopts':cnopts})
        else:
            raise NotImplementedError('only s3 and ftp supported')

        return cnxn

    def _disconnect(self, cnxn):
        if self.settings['protocol'] == 'ftp':
            cnxn.close_del()

