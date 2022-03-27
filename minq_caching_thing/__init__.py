
import os
import hashlib
import threading
import time

class Minq_caching_thing:

    # static settings

    # folders
    cache_dir = os.path.expanduser('~/.cache/minq-caching-thing')
    hashed_bytes_dir = os.path.join(cache_dir, 'hashes', 'sha512')
    url_associations_dir = os.path.join(cache_dir, 'url-associations')
    # files
    hash_is_verified_file = 'hash-is-verified'
    hash_is_being_processed_file = 'hash-is-being-processes'
    hash_is_being_processed_file_lifespan_limit = 60 * 60 * 5 # in seconds
    hash_content_file = 'content'
    url_content_file = 'points-to'
    url_is_verified_file = 'url-is-verified'
    # encoding specific
    hashing_algorithm = hashlib.sha512
    string_encoder = 'UTF-8'

    # static functions

    def get_bytes_hash(s, data):
        assert type(data) == bytes
        m = s.hashing_algorithm()
        m.update(data)
        return m.hexdigest()
    
    # less static functions
    
    def get_url_dir(s, url):
        resulting_dir = s.url_associations_dir
        for char in url: # TODO this could be improved
            resulting_dir = os.path.join(resulting_dir, str(ord(char)))
        return resulting_dir

    # not statuc functions

    def cache(s, *a, blocking=False, **kw):
        if blocking:
            return s._cache_thread(*a, **kw)
        else:
            thr = threading.Thread(target=s._cache_thread, args=a, kwargs=kw)
            thr.start()
            return thr
    def _cache_thread(s, data, hash_=None):
        if type(data) == bytes:
            pass
        elif type(data) == str:
            data = data.encode(s.string_encoder)
        else:
             assert False
        if hash_ == None:
            hash_ = s.get_bytes_hash(data)
        hash_dir = os.path.join(s.hashed_bytes_dir, hash_)
        content_file = os.path.join(hash_dir, s.hash_content_file)
        verification_file = os.path.join(hash_dir, s.hash_is_verified_file)
        if not os.path.isfile(verification_file):
            being_processed_file = os.path.join(s.hashed_bytes_dir, s.hash_is_being_processed_file)
            if os.path.isfile(being_processed_file): # this is not perfect
                now = time.time()
                last_modified = os.path.getmtime(being_processes_file)
                if last_modified + s.hash_is_being_processed_file_lifespan_limit < now:
                    os.remove(being_processed_file)
                else:
                    #return
                    assert False, 'if this ever occurs, please ask the developer to do something about it'
            else:
                os.makedirs(hash_dir) # this is questionable
            with open(being_processed_file, 'w') as f:
                pass
            with open(content_file, 'wb') as f:
                f.write(data)
            with open(verification_file, 'w') as f:
                pass
            os.remove(being_processed_file)
        return content_file
    
    def get_cache(s, hash_, return_path=False, return_file_obj=False, read_mode=''):
        hash_dir = os.path.join(s.hashed_bytes_dir, hash_)
        verification_file = os.path.join(hash_dir, s.hash_is_verified_file)
        if not os.path.isfile(verification_file):
            return
        content_file = os.path.join(hash_dir, s.hash_content_file)
        if return_path:
            match read_mode:
                case '': pass
                case 'b': content_file = content_file.encode()
                case _: raise # unreachable
            return content_file
        f = open(content_file, 'r'+read_mode) # what if content_file doesn't exist?
        if return_file_obj:
            return f
        with f as f:
            return f.read()
    
    def cache_url(s, *a, blocking=False, **kw):
        kw.update({'blocking':blocking})
        if blocking:
            return s._cache_url_thread(*a, **kw)
        else:
            thr = threading.Thread(target=s._cache_url_thread, args=a, kwargs=kw)
            thr.start()
            return thr
    def _cache_url_thread(s, url, data):
        if type(data) == str:
            data = data.encode(s.string_encoder)
        hash_ = s.get_bytes_hash(data)
        cache_data_thr = s.cache(data, hash_=hash_)
        url_dir = s.get_url_dir(url)
        verified_file = os.path.join(url_dir, s.url_is_verified_file)
        if os.path.isfile(verified_file):
            os.remove(verified_file)
        else:
            if not os.path.isdir(url_dir):
                os.makedirs(url_dir)
        content_file = os.path.join(url_dir, s.url_content_file)
        with open(content_file, 'w') as f: # bad, we need some checks due to race conditions
            f.write(hash_)
        with open(verified_file, 'w'):
            pass
        cache_data_thr.join()

    def get_url(s, url, *a, **kw):
        url_dir = s.get_url_dir(url)
        verified_file = os.path.join(url_dir, s.url_is_verified_file)
        if not os.path.isfile(verified_file):
            return
        content_file = os.path.join(url_dir, s.url_content_file)
        with open(content_file, 'r') as f:
            hash_ = f.read()
        return s.get_cache(hash_, *a, **kw)

if __name__ == '__main__':
    print('Running self test')
    mct = Minq_caching_thing()
    mct.cache('just a simple test')
    mct.cache_url('test', 'asdfgh')
    time.sleep(10)
