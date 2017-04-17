import hashlib


def hash_str(s, method='md5'):
    """Hashes a URL

    :param s: url
    :type s: str
    :param method: hashing method {'md5', 'sha512'}
    :type method: str
    :return: hash hex digest
    :rtype: str
    """

    link_str = s if type(s) is bytes else s.encode('utf-8')
    h = hashlib.__dict__[method]()
    h.update(link_str)
    return h.hexdigest()
