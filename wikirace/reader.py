from bs4 import BeautifulSoup
from wikirace.configuration import DMN

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen


def make_wiki_link(url):
    if url.startswith('/wiki'):
        return '{}{}'.format(DMN, url)
    else:
        return url


def make_tmplt(url, title=None, parent=None, dst=None, lnt=0, src=None):
    tmpl = {
        # 'url': '{}/{}'.format(DMN, url),
        'url': make_wiki_link(url),
        'title': title,
        'parent': parent if parent else list(),
        'dst': dst,
        'lnt': lnt
    }

    tmpl['src'] = src if src else tmpl['url']
    return tmpl


def get_links(url):
    print(url)
    try:
        page = urlopen(url)
    except:
        return []

    soup = BeautifulSoup(page.read())
    links_tmp = soup.findAll("a")
    links = list()
    for link in links_tmp:
        if str(link.attrs.get('href')).startswith('/wiki') and not link.attrs.get('class'):
            links.append({
                'url': link.attrs.get('href'),
                'title': link.attrs.get('title')
            })

    return links

if __name__ == '__main__':
    url = 'https://en.wikipedia.org/wiki/Western_Collegiate_Hockey_Association'
    urls = get_links(url)
    print(urls)
    print(len(urls))
